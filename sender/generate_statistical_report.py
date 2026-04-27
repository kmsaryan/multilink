#!/usr/bin/env python3
import argparse
import fnmatch
import math
import os
import re
import sqlite3
import statistics
import sys
import time
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import config
from db_utils import (
    infer_scenario_from_filename,
    init_sender_db,
    store_checkpoint_statistics,
    store_run_statistics,
    store_scenario_statistics,
)

def safe_mean(values: List[float]) -> Optional[float]:
    return statistics.mean(values) if values else None


def safe_stdev(values: List[float]) -> Optional[float]:
    return statistics.stdev(values) if len(values) >= 2 else None


def ci95_half_width(values: List[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    sd = statistics.stdev(values)
    return 1.96 * sd / math.sqrt(len(values))


def fmt_number(value: Optional[float], digits: int = 3) -> str:
    if value is None:
        return "NA"
    return f"{value:.{digits}f}"


def safe_variance(values: List[float]) -> Optional[float]:
    return statistics.variance(values) if len(values) >= 2 else None


def metric_stats(values: List[float]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    return safe_mean(values), safe_variance(values), safe_stdev(values)


def numeric_run_key(filename: str) -> Tuple[int, str]:
    match = re.search(r"(\d+)", filename or "")
    if not match:
        return (10**9, filename or "")
    return (int(match.group(1)), filename or "")


def matches_filename_pattern(filename: str, pattern: Optional[str]) -> bool:
    if not pattern:
        return True
    return fnmatch.fnmatchcase(filename or "", pattern)


def select_checkpoints(n_payloads: int, step: int, max_files: Optional[int]) -> List[int]:
    if n_payloads <= 0:
        return []

    step_value = max(1, int(step))
    max_limit = n_payloads if max_files is None else max(1, min(n_payloads, int(max_files)))

    checkpoints = list(range(step_value, max_limit + 1, step_value))
    if max_limit not in checkpoints:
        checkpoints.append(max_limit)

    if n_payloads > max_limit and n_payloads not in checkpoints:
        checkpoints.append(n_payloads)

    return sorted(set(checkpoints))


def build_cumulative_file_rows(
    payload_rows: Sequence[Dict[str, object]],
    checkpoint_step: int,
    max_files: Optional[int],
    scenario_name: str = "overall",
) -> List[Dict[str, object]]:
    sorted_rows = sorted(payload_rows, key=lambda row: numeric_run_key(str(row.get("filename", ""))))
    checkpoints = select_checkpoints(len(sorted_rows), checkpoint_step, max_files)
    metric_specs = [
        ("send_span_s", "send_span_s"),
        ("avg_attempts", "avg_attempts"),
        ("max_attempts", "max_attempts"),
    ]

    output_rows: List[Dict[str, object]] = []
    for checkpoint in checkpoints:
        subset = sorted_rows[:checkpoint]
        for metric_name, metric_key in metric_specs:
            values = [float(row[metric_key]) for row in subset if row.get(metric_key) is not None]
            mean_value, variance_value, std_value = metric_stats(values)
            output_rows.append(
                {
                    "scenario": scenario_name,
                    "file_count": checkpoint,
                    "metric_column": metric_name,
                    "sample_count": len(values),
                    "mean": mean_value,
                    "variance": variance_value,
                    "std": std_value,
                }
            )
    return output_rows


def compute_stability_delta(reference: Optional[float], final_value: Optional[float]) -> Optional[float]:
    if reference is None or final_value is None:
        return None
    if final_value == 0:
        return 0.0 if reference == 0 else None
    return abs(reference - final_value) / abs(final_value) * 100.0


def variance_stability_series(values_by_checkpoint: Sequence[Tuple[int, float]]) -> List[Dict[str, object]]:
    """Compute relative change in variance between consecutive checkpoints."""
    result: List[Dict[str, object]] = []
    prev_variance: Optional[float] = None
    for file_count, variance in values_by_checkpoint:
        if prev_variance is None or prev_variance == 0:
            delta_pct = None
        else:
            delta_pct = abs(variance - prev_variance) / prev_variance * 100.0
        result.append({"file_count": int(file_count), "variance": variance, "delta_pct": delta_pct})
        prev_variance = variance
    return result


def find_variance_stability_point(
    stability_series: Sequence[Dict[str, object]], threshold_pct: float = 5.0
) -> Tuple[Optional[int], Optional[float]]:
    """Return the first checkpoint where variance stays stable for two steps."""
    for i, row in enumerate(stability_series):
        delta = row.get("delta_pct")
        if delta is None or delta > threshold_pct:
            continue
        if i + 1 < len(stability_series):
            next_delta = stability_series[i + 1].get("delta_pct")
            if next_delta is not None and next_delta <= threshold_pct:
                return int(row["file_count"]), float(delta)
        else:
            return int(row["file_count"]), float(delta)
    return None, None


def build_scenario_significance_rows(
    payload_rows: Sequence[Dict[str, object]],
    checkpoint_step: int,
    max_files: Optional[int],
) -> List[Dict[str, object]]:
    scenario_groups: Dict[str, List[Dict[str, object]]] = {}
    for row in payload_rows:
        scenario_name = str(row.get("scenario") or "unknown")
        scenario_groups.setdefault(scenario_name, []).append(row)

    scenario_rows: List[Dict[str, object]] = []
    for scenario_name, rows in sorted(scenario_groups.items(), key=lambda item: item[0]):
        ordered_rows = sorted(rows, key=lambda row: numeric_run_key(str(row.get("filename", ""))))
        transfer_times = [float(row["send_span_s"]) for row in ordered_rows if row.get("send_span_s") is not None]
        if not transfer_times:
            continue

        scenario_checkpoint_rows = build_cumulative_file_rows(
            ordered_rows,
            checkpoint_step=checkpoint_step,
            max_files=max_files,
            scenario_name=scenario_name,
        )

        variance_series = variance_stability_series(
            [
                (int(row["file_count"]), float(row["variance"]))
                for row in scenario_checkpoint_rows
                if row.get("metric_column") == "send_span_s" and row.get("variance") is not None
            ]
        )
        stable_k, stable_delta = find_variance_stability_point(variance_series, threshold_pct=5.0)

        mean_value, variance_value, std_value = metric_stats(transfer_times)
        ci95_value = ci95_half_width(transfer_times)
        cv_pct = (std_value / mean_value * 100.0) if std_value is not None and mean_value not in (None, 0) else None

        if stable_k is not None:
            significance_flag = "stable"
            significance_note = (
                f"Var(X_k) stabilizes at k={stable_k} "
                f"(relative change < 5% between consecutive checkpoints)"
            )
        elif len(transfer_times) >= 32:
            significance_flag = "sufficient_32"
            significance_note = (
                "32+ samples collected — CLT approximations valid "
                "even without full variance convergence"
            )
        else:
            significance_flag = "insufficient"
            significance_note = (
                f"Only {len(transfer_times)} samples — "
                "variance has not stabilized, collect more data"
            )

        scenario_rows.append(
            {
                "scenario": scenario_name,
                "sample_count": len(transfer_times),
                "mean_transfer_time_s": mean_value,
                "variance_transfer_time_s": variance_value,
                "std_transfer_time_s": std_value,
                "ci95_half_width_s": ci95_value,
                "cv_pct": cv_pct,
                "stable_k": stable_k,
                "variance_stability_delta_pct": stable_delta,
                "significance_flag": significance_flag,
                "significance_note": significance_note,
            }
        )

    return scenario_rows


def table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (name,),
    ).fetchone()
    return row is not None


def fetch_payload_rows(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    query = """
        SELECT payload_id, filename, total_chunks, status
        FROM payloads
        ORDER BY rowid ASC
    """
    return list(conn.execute(query).fetchall())


def fetch_incomplete_payloads(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    query = """
        SELECT
            p.payload_id,
            p.filename,
            COUNT(c.idx) AS total_chunk_rows,
            SUM(CASE WHEN c.state='acked' THEN 1 ELSE 0 END) AS acked_chunk_rows,
            SUM(CASE WHEN c.state!='acked' OR c.state IS NULL THEN 1 ELSE 0 END) AS unacked_chunk_rows
        FROM payloads p
        LEFT JOIN chunks c ON c.payload_id = p.payload_id
        GROUP BY p.payload_id, p.filename
        HAVING SUM(CASE WHEN c.state!='acked' OR c.state IS NULL THEN 1 ELSE 0 END) > 0
        ORDER BY p.rowid ASC
    """
    return list(conn.execute(query).fetchall())


def fetch_chunk_stats(conn: sqlite3.Connection, payload_id: str) -> Dict[str, object]:
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN state='acked' THEN 1 ELSE 0 END) AS acked_rows,
            SUM(CASE WHEN state='pending' THEN 1 ELSE 0 END) AS pending_rows,
            SUM(CASE WHEN state='sending' THEN 1 ELSE 0 END) AS sending_rows,
            AVG(COALESCE(attempts, 0)) AS avg_attempts,
            MAX(COALESCE(attempts, 0)) AS max_attempts,
            MIN(last_sent) AS first_last_sent,
            MAX(last_sent) AS last_last_sent
        FROM chunks
        WHERE payload_id = ?
        """,
        (payload_id,),
    ).fetchone()

    iface_rows = conn.execute(
        """
        SELECT assigned_interface, COUNT(*) AS cnt
        FROM chunks
        WHERE payload_id = ? AND assigned_interface IS NOT NULL
        GROUP BY assigned_interface
        """,
        (payload_id,),
    ).fetchall()

    interface_distribution = {str(r[0]): int(r[1]) for r in iface_rows}

    return {
        "total_rows": int(row[0] or 0),
        "acked_rows": int(row[1] or 0),
        "pending_rows": int(row[2] or 0),
        "sending_rows": int(row[3] or 0),
        "avg_attempts": float(row[4]) if row[4] is not None else None,
        "max_attempts": int(row[5] or 0),
        "first_last_sent": row[6],
        "last_last_sent": row[7],
        "interface_distribution": interface_distribution,
    }


def print_and_write_output(content_lines: List[str], output_file_handle=None):
    """Helper function to print to stdout and optionally write to a file."""
    for line in content_lines:
        print(line)
        if output_file_handle:
            output_file_handle.write(line + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sender-only statistical report from sender DB.")
    parser.add_argument("--sender-db", default=config.DB_PATH, help="Path to sender SQLite DB.")
    parser.add_argument("--output-file", help="Optional path to write output to a text file.")
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Allow report generation even when some chunks are not yet acked.",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Keep polling DB and generate reports continuously when data is available.",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=2.0,
        help="Polling interval in seconds for --watch mode.",
    )
    parser.add_argument(
        "--checkpoint-step",
        type=int,
        default=2,
        help="Checkpoint increment in number of files (e.g., 2 -> 2,4,6,...).",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=50,
        help="Maximum file-count checkpoint to evaluate (capped by available payloads).",
    )
    parser.add_argument(
        "--filename-pattern",
        default=None,
        help=(
            "Optional shell-style filename filter applied to payload filenames "
            "(e.g., 'Nlos_LinkFail2_*.data')."
        ),
    )
    args = parser.parse_args()

    if not os.path.exists(args.sender_db):
        raise SystemExit(f"Sender DB not found: {args.sender_db}")

    init_sender_db(args.sender_db)

    while True:
        conn = sqlite3.connect(args.sender_db)
        conn.row_factory = sqlite3.Row

        if not table_exists(conn, "payloads") or not table_exists(conn, "chunks"):
            conn.close()
            raise SystemExit("Sender DB missing required tables `payloads` or `chunks`.")

        payload_base_rows = fetch_payload_rows(conn)
        if args.filename_pattern:
            payload_base_rows = [
                row for row in payload_base_rows if matches_filename_pattern(str(row["filename"] or ""), args.filename_pattern)
            ]
        if not payload_base_rows:
            conn.close()
            if args.watch:
                print(f"[watch] No payload rows yet. Retrying in {args.poll_interval}s...")
                time.sleep(args.poll_interval)
                continue
            if args.filename_pattern:
                raise SystemExit(
                    f"No payload rows matched --filename-pattern '{args.filename_pattern}' in sender DB."
                )
            raise SystemExit("No payload rows found in sender DB.")

        if not args.allow_partial:
            incomplete_payloads = fetch_incomplete_payloads(conn)
            if incomplete_payloads:
                conn.close()
                if args.watch:
                    print(f"[watch] Waiting for ACK completion ({len(incomplete_payloads)} incomplete payloads)...")
                    time.sleep(args.poll_interval)
                    continue
                print("Report generation blocked: some payloads are not fully acked yet.")
                print("Incomplete payloads:")
                for row in incomplete_payloads[:10]:
                    total_rows = int(row["total_chunk_rows"] or 0)
                    acked_rows = int(row["acked_chunk_rows"] or 0)
                    print(f" - {row['filename']} ({row['payload_id']}): {acked_rows}/{total_rows} chunks acked")
                if len(incomplete_payloads) > 10:
                    print(f" ... and {len(incomplete_payloads) - 10} more")
                raise SystemExit("Run report again after all chunks are acked, or use --allow-partial.")

        per_payload_rows: List[Dict[str, object]] = []
        iface_distribution_rows: List[Dict[str, object]] = []
        scenario_transfer_times: Dict[str, List[float]] = {}

        for row in payload_base_rows:
            payload_id = row["payload_id"]
            filename = row["filename"]
            total_chunks_declared = int(row["total_chunks"] or 0)
            status = row["status"]
            scenario = infer_scenario_from_filename(filename)

            chunk_stats = fetch_chunk_stats(conn, payload_id)
            total_rows = int(chunk_stats["total_rows"])
            acked_rows = int(chunk_stats["acked_rows"])
            pending_rows = int(chunk_stats["pending_rows"])
            sending_rows = int(chunk_stats["sending_rows"])

            acked_ratio_pct = (acked_rows / total_rows * 100) if total_rows > 0 else 0.0
            send_span_s = None
            first_last_sent = chunk_stats["first_last_sent"]
            last_last_sent = chunk_stats["last_last_sent"]
            if first_last_sent is not None and last_last_sent is not None and last_last_sent >= first_last_sent:
                send_span_s = float(last_last_sent - first_last_sent)

            store_run_statistics(
                args.sender_db,
                payload_id,
                filename,
                scenario,
                total_chunks_declared,
                total_rows,
                acked_rows,
                pending_rows,
                sending_rows,
                acked_ratio_pct,
                chunk_stats["avg_attempts"],
                chunk_stats["max_attempts"],
                send_span_s,
                first_last_sent,
                last_last_sent,
            )

            if send_span_s is not None:
                scenario_transfer_times.setdefault(scenario, []).append(send_span_s)

            per_payload_rows.append(
                {
                    "payload_id": payload_id,
                    "filename": filename,
                    "scenario": scenario,
                    "status": status,
                    "total_chunks_declared": total_chunks_declared,
                    "chunks_rows": total_rows,
                    "acked_rows": acked_rows,
                    "pending_rows": pending_rows,
                    "sending_rows": sending_rows,
                    "acked_ratio_pct": acked_ratio_pct,
                    "avg_attempts": chunk_stats["avg_attempts"],
                    "max_attempts": chunk_stats["max_attempts"],
                    "send_span_s": send_span_s,
                    "first_last_sent": first_last_sent,
                    "last_last_sent": last_last_sent,
                }
            )

            distribution = chunk_stats["interface_distribution"]
            total_assigned = sum(distribution.values())
            for interface_ip, count in distribution.items():
                iface_distribution_rows.append(
                    {
                        "payload_id": payload_id,
                        "interface_ip": interface_ip,
                        "assigned_chunks": count,
                        "assigned_share_pct": (count / total_assigned * 100) if total_assigned > 0 else 0.0,
                    }
                )

        scenario_snapshot_rows: List[Dict[str, object]] = []
        for scenario, transfer_times in scenario_transfer_times.items():
            if not transfer_times:
                continue
            mean_value = safe_mean(transfer_times)
            variance_value = statistics.variance(transfer_times) if len(transfer_times) >= 2 else None
            std_value = safe_stdev(transfer_times)
            min_value = min(transfer_times)
            max_value = max(transfer_times)
            ci95_value = ci95_half_width(transfer_times)

            store_scenario_statistics(args.sender_db, scenario, transfer_times, source_payload_count=len(transfer_times))

            scenario_snapshot_rows.append(
                {
                    "scenario": scenario,
                    "sample_count": len(transfer_times),
                    "mean_transfer_time_s": mean_value,
                    "variance_transfer_time_s": variance_value,
                    "std_transfer_time_s": std_value,
                    "min_transfer_time_s": min_value,
                    "max_transfer_time_s": max_value,
                    "ci95_half_width_s": ci95_value,
                    "source_payload_count": len(transfer_times),
                }
            )

        scenario_significance_rows = build_scenario_significance_rows(
            per_payload_rows,
            checkpoint_step=args.checkpoint_step,
            max_files=args.max_files,
        )

        report_run_id = str(uuid.uuid4())
        scenario_groups: Dict[str, List[Dict[str, object]]] = {}
        for row in per_payload_rows:
            scenario_name = str(row.get("scenario") or "unknown")
            scenario_groups.setdefault(scenario_name, []).append(row)

        for scenario_name, scenario_rows in sorted(scenario_groups.items(), key=lambda item: item[0]):
            cumulative_rows = build_cumulative_file_rows(
                scenario_rows,
                checkpoint_step=args.checkpoint_step,
                max_files=args.max_files,
                scenario_name=scenario_name,
            )
            for row in cumulative_rows:
                store_checkpoint_statistics(
                    args.sender_db,
                    report_run_id=report_run_id,
                    scenario=str(row.get("scenario") or scenario_name or "unknown"),
                    metric_column=str(row.get("metric_column") or "unknown"),
                    file_count=int(row.get("file_count") or 0),
                    sample_count=int(row.get("sample_count") or 0),
                    mean_value=float(row["mean"]) if row.get("mean") is not None else None,
                    variance_value=float(row["variance"]) if row.get("variance") is not None else None,
                    std_value=float(row["std"]) if row.get("std") is not None else None,
                )

        iface_health_rows: List[Dict[str, object]] = []
        if table_exists(conn, "interface_stats"):
            rows = conn.execute(
                """
                SELECT interface_ip, avg_rtt, success_rate, jitter, loss_rate, instant_bitrate, performance_score, last_check
                FROM interface_stats
                ORDER BY interface_ip ASC
                """
            ).fetchall()
            for row in rows:
                iface_health_rows.append(
                    {
                        "interface_ip": row[0],
                        "avg_rtt_ms": row[1],
                        "throughput_bps": row[2],
                        "jitter_ms": row[3],
                        "loss_rate_pct": row[4],
                        "instant_bitrate": row[5],
                        "performance_score": row[6],
                        "last_check": row[7],
                    }
                )

        prediction_rows: List[Dict[str, object]] = []
        if table_exists(conn, "interface_predictions"):
            rows = conn.execute(
                """
                SELECT interface_ip, predicted_rtt, predicted_bitrate, avg_jitter, avg_loss, blended_score, timestamp
                FROM interface_predictions
                ORDER BY interface_ip ASC
                """
            ).fetchall()
            for row in rows:
                prediction_rows.append(
                    {
                        "interface_ip": row[0],
                        "predicted_rtt": row[1],
                        "predicted_bitrate": row[2],
                        "avg_jitter": row[3],
                        "avg_loss": row[4],
                        "blended_score": row[5],
                        "timestamp": row[6],
                    }
                )

        conn.close()

        # --- Prepare Output Content ---
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append("SENDER STATISTICAL REPORT SUMMARY")
        output_lines.append("=" * 80)
        output_lines.append("")

        # Scenario Statistics Snapshots
        if scenario_snapshot_rows:
            output_lines.append("SCENARIO STATISTICS SNAPSHOT (Aggregated per Scenario):")
            output_lines.append(f"  {'Scenario':<20} {'Samples':<8} {'Mean (s)':<10} {'Variance':<12} {'Std Dev (s)':<12} {'Min (s)':<10} {'Max (s)':<10}")
            for row in scenario_snapshot_rows:
                output_lines.append(
                    f"  {row['scenario']:<20} {row['sample_count']:<8} {fmt_number(row['mean_transfer_time_s']):<10} {fmt_number(row['variance_transfer_time_s']):<12} {fmt_number(row['std_transfer_time_s']):<12} {fmt_number(row['min_transfer_time_s']):<10} {fmt_number(row['max_transfer_time_s']):<10}"
                )
            output_lines.append("") # Empty line

        # Scenario Significance Summary
        output_lines.append("SCENARIO SIGNIFICANCE SUMMARY:")
        output_lines.append(f"  {'Scenario':<20} {'Files':<6} {'Mean (s)':<10} {'Var (s²)':<12} {'Stable k':<10} {'Significance Flag':<20}")
        for row in scenario_significance_rows:
            stable_k_str = fmt_number(row.get('stable_k'), 0) if row.get('stable_k') is not None else 'N/A'
            output_lines.append(
                f"  {row['scenario']:<20} {row['sample_count']:<6} {fmt_number(row.get('mean_transfer_time_s')):<10} {fmt_number(row.get('variance_transfer_time_s')):<12} {stable_k_str:<10} {row['significance_flag']:<20}"
            )
        output_lines.append("") # Empty line
        output_lines.append("=" * 80)

        # --- Print/Write Output ---
        report_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "modeling_reports",
            "statistical_reports",
        )
        os.makedirs(report_dir, exist_ok=True)
        scenario_labels = sorted({str(row.get("scenario") or "unknown") for row in scenario_significance_rows})
        scenario_name = "_".join(scenario_labels) if scenario_labels else "overall"
        safe_scenario = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in scenario_name)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        auto_output_file = os.path.join(report_dir, f"statistical_report_{safe_scenario}_{timestamp}.txt")

        if args.output_file:
             with open(args.output_file, 'w') as f:
                 print_and_write_output(output_lines, f)
        else:
             print_and_write_output(output_lines)

        with open(auto_output_file, "w") as f:
            for line in output_lines:
                f.write(line + "\n")

        print("") # Extra newline after report
        print(f"Statistical report generated (printed to {'file' if args.output_file else 'console'}).")
        print(f"Snapshot report saved: {auto_output_file}")

        if not args.watch:
            break

        time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
