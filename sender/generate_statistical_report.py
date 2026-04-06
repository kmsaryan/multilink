#!/usr/bin/env python3
import argparse
import csv
import math
import os
import re
import sqlite3
import statistics
import time
import uuid
from typing import Dict, List, Optional, Sequence, Tuple

import config
from db_utils import (
    infer_scenario_from_filename,
    init_sender_db,
    store_checkpoint_statistics,
    store_run_statistics,
    store_scenario_statistics,
)


DEFAULT_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "modeling_reports", "statistical_reports")


def safe_mean(values: List[float]) -> Optional[float]:
    return statistics.mean(values) if values else None


def safe_stdev(values: List[float]) -> Optional[float]:
    return statistics.stdev(values) if len(values) >= 2 else None


def ci95_half_width(values: List[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    sd = statistics.stdev(values)
    return 1.96 * sd / math.sqrt(len(values))


def write_csv(path: str, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


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


def find_mean_variance_closest_checkpoint(
    checkpoint_rows: Sequence[Dict[str, object]],
    metric_column: str,
) -> Optional[Dict[str, object]]:
    candidates: List[Dict[str, object]] = []
    for row in checkpoint_rows:
        if row.get("metric_column") != metric_column:
            continue
        mean_value = row.get("mean")
        std_value = row.get("std")
        if mean_value is None or std_value is None:
            continue
        abs_gap = abs(float(mean_value) - float(std_value))
        denom = max(abs(float(mean_value)), abs(float(std_value)), 1e-9)
        rel_gap_pct = abs_gap / denom * 100.0
        candidates.append(
            {
                **row,
                "mean_std_abs_gap": abs_gap,
                "mean_std_rel_gap_pct": rel_gap_pct,
            }
        )

    if not candidates:
        return None

    return min(candidates, key=lambda row: (float(row["mean_std_abs_gap"]), int(row["file_count"])))


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
        closest_row = find_mean_variance_closest_checkpoint(scenario_checkpoint_rows, metric_column="send_span_s")

        mean_value, variance_value, std_value = metric_stats(transfer_times)
        ci95_value = ci95_half_width(transfer_times)
        cv_pct = (std_value / mean_value * 100.0) if std_value is not None and mean_value not in (None, 0) else None

        baseline_32_mean = None
        baseline_32_variance = None
        if len(transfer_times) >= 32:
            first_32 = transfer_times[:32]
            baseline_32_mean = safe_mean(first_32)
            baseline_32_variance = safe_variance(first_32)

        mean_delta_pct = compute_stability_delta(baseline_32_mean, mean_value)
        variance_delta_pct = compute_stability_delta(baseline_32_variance, variance_value)

        stable_mean = mean_delta_pct is not None and mean_delta_pct <= 5.0
        stable_variance = variance_delta_pct is not None and variance_delta_pct <= 10.0

        if closest_row is not None:
            significance_flag = "closest_mean_std"
            significance_note = (
                f"closest mean≈std at file_count={int(closest_row['file_count'])} "
                f"(abs_gap={closest_row['mean_std_abs_gap']:.3f})"
            )
        elif mean_delta_pct is None or variance_delta_pct is None:
            significance_flag = "insufficient"
            significance_note = "insufficient samples for significance check"
        elif stable_mean and stable_variance:
            significance_flag = "stable"
            significance_note = "32-file and final statistics are close"
        else:
            significance_flag = "drifting"
            significance_note = "material shift observed after 32 files"

        scenario_rows.append(
            {
                "scenario": scenario_name,
                "sample_count": len(transfer_times),
                "mean_transfer_time_s": mean_value,
                "variance_transfer_time_s": variance_value,
                "std_transfer_time_s": std_value,
                "ci95_half_width_s": ci95_value,
                "cv_pct": cv_pct,
                "mean_delta_32_to_final_pct": mean_delta_pct,
                "variance_delta_32_to_final_pct": variance_delta_pct,
                "closest_mean_std_file_count": closest_row["file_count"] if closest_row else None,
                "closest_mean_std_abs_gap": closest_row["mean_std_abs_gap"] if closest_row else None,
                "closest_mean_std_rel_gap_pct": closest_row["mean_std_rel_gap_pct"] if closest_row else None,
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


def build_markdown_summary(
    out_path: str,
    n_payloads: int,
    payload_rows: List[Dict[str, object]],
    sender_csv: str,
    iface_csv: str,
    prediction_csv: str,
    scenario_csv: str,
    cumulative_csv: str,
    scenario_significance_csv: str,
    cumulative_rows: List[Dict[str, object]],
    scenario_significance_rows: List[Dict[str, object]],
) -> None:
    ack_rates = [float(r["acked_ratio_pct"]) for r in payload_rows if r.get("acked_ratio_pct") is not None]
    send_spans = [float(r["send_span_s"]) for r in payload_rows if r.get("send_span_s") is not None]

    lines = []
    lines.append("# Sender Statistical Results Summary")
    lines.append("")
    lines.append(f"- Total payloads analysed: **{n_payloads}**")
    lines.append(f"- Per-payload sender metrics: `{sender_csv}`")
    lines.append(f"- Interface health summary: `{iface_csv}`")
    lines.append(f"- Prediction summary: `{prediction_csv}`")
    lines.append(f"- Scenario significance snapshot: `{scenario_csv}`")
    lines.append(f"- Cumulative file-count statistics: `{cumulative_csv}`")
    lines.append(f"- Scenario significance table: `{scenario_significance_csv}`")
    lines.append("")
    lines.append("## Sender KPIs")
    lines.append("")
    lines.append(f"- Mean chunk ACK ratio: **{fmt_number(safe_mean(ack_rates), 2)}%**")
    lines.append(f"- Mean sender-side send span: **{fmt_number(safe_mean(send_spans), 3)} s**")
    lines.append(f"- Send span CI95: **±{fmt_number(ci95_half_width(send_spans), 3)} s**")
    lines.append("")
    lines.append("## Table 1: File-count checkpoints (mean | variance | std)")
    lines.append("")
    lines.append("| files | relevant_column | mean | variance | std |")
    lines.append("|---:|---|---:|---:|---:|")
    for row in cumulative_rows:
        lines.append(
            f"| {int(row['file_count'])} | {row['metric_column']} | {fmt_number(row.get('mean'))} | {fmt_number(row.get('variance'))} | {fmt_number(row.get('std'))} |"
        )
    lines.append("")
    lines.append("## Table 2: Scenario-based significance")
    lines.append("")
    lines.append("| scenario | files | mean | variance | std | closest file(mean≈std) | abs gap | rel gap % | significance |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---|")
    for row in scenario_significance_rows:
        lines.append(
            f"| {row['scenario']} | {int(row['sample_count'])} | {fmt_number(row.get('mean_transfer_time_s'))} | {fmt_number(row.get('variance_transfer_time_s'))} | {fmt_number(row.get('std_transfer_time_s'))} | {fmt_number(row.get('closest_mean_std_file_count'), 0)} | {fmt_number(row.get('closest_mean_std_abs_gap'))} | {fmt_number(row.get('closest_mean_std_rel_gap_pct'), 2)} | {row['significance_note']} |"
        )
    lines.append("")
    lines.append("## Stability Summary")
    lines.append("")
    stable_rows = [row for row in scenario_significance_rows if row.get("significance_flag") == "stable"]
    if stable_rows:
        lines.append(
            "- At 32 files, mean and variance are close to final values for: "
            + ", ".join(sorted(str(row["scenario"]) for row in stable_rows))
            + "."
        )
    else:
        lines.append("- At 32 files, no scenario met the configured closeness thresholds for both mean and variance.")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This report is sender-only and uses only sender DB tables (`payloads`, `chunks`, `interface_stats`, `interface_metrics_history`, `interface_predictions`).")
    lines.append("- `send_span_s` is computed from chunk `last_sent` min/max and represents sender transmission activity span, not receiver completion time.")
    lines.append("- `run_statistics` stores one row per payload/run, while `scenario_statistics` stores appended snapshots for each scenario.")
    lines.append("- Significance target is based on mean≈std closeness at checkpoint level for `send_span_s` (unit-consistent in seconds).")
    lines.append("- Checkpoints are built by `--checkpoint-step` and `--max-files`; snapshots are appended to `checkpoint_statistics_history`.")

    with open(out_path, "w") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sender-only statistical report from sender DB.")
    parser.add_argument("--sender-db", default=config.DB_PATH, help="Path to sender SQLite DB.")
    parser.add_argument("--out-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory for CSV/MD summary.")
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
    args = parser.parse_args()

    if not os.path.exists(args.sender_db):
        raise SystemExit(f"Sender DB not found: {args.sender_db}")

    os.makedirs(args.out_dir, exist_ok=True)
    init_sender_db(args.sender_db)

    while True:
        conn = sqlite3.connect(args.sender_db)
        conn.row_factory = sqlite3.Row

        if not table_exists(conn, "payloads") or not table_exists(conn, "chunks"):
            conn.close()
            raise SystemExit("Sender DB missing required tables `payloads` or `chunks`.")

        payload_base_rows = fetch_payload_rows(conn)
        if not payload_base_rows:
            conn.close()
            if args.watch:
                print(f"[watch] No payload rows yet. Retrying in {args.poll_interval}s...")
                time.sleep(args.poll_interval)
                continue
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

        cumulative_rows = build_cumulative_file_rows(
            per_payload_rows,
            checkpoint_step=args.checkpoint_step,
            max_files=args.max_files,
            scenario_name="overall",
        )
        scenario_significance_rows = build_scenario_significance_rows(
            per_payload_rows,
            checkpoint_step=args.checkpoint_step,
            max_files=args.max_files,
        )

        report_run_id = str(uuid.uuid4())
        for row in cumulative_rows:
            store_checkpoint_statistics(
                args.sender_db,
                report_run_id=report_run_id,
                scenario=str(row.get("scenario") or "overall"),
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

        sender_csv = os.path.join(args.out_dir, "sender_per_payload_metrics.csv")
        iface_dist_csv = os.path.join(args.out_dir, "sender_interface_assignment.csv")
        iface_health_csv = os.path.join(args.out_dir, "sender_interface_health.csv")
        prediction_csv = os.path.join(args.out_dir, "sender_prediction_snapshot.csv")
        scenario_csv = os.path.join(args.out_dir, "scenario_statistics_snapshot.csv")
        cumulative_csv = os.path.join(args.out_dir, "cumulative_file_count_statistics.csv")
        scenario_significance_csv = os.path.join(args.out_dir, "scenario_significance_summary.csv")
        summary_md = os.path.join(args.out_dir, "sender_statistical_summary.md")

        write_csv(
            sender_csv,
            per_payload_rows,
            [
                "payload_id",
                "filename",
                "scenario",
                "status",
                "total_chunks_declared",
                "chunks_rows",
                "acked_rows",
                "pending_rows",
                "sending_rows",
                "acked_ratio_pct",
                "avg_attempts",
                "max_attempts",
                "send_span_s",
                "first_last_sent",
                "last_last_sent",
            ],
        )

        write_csv(
            iface_dist_csv,
            iface_distribution_rows,
            ["payload_id", "interface_ip", "assigned_chunks", "assigned_share_pct"],
        )

        write_csv(
            iface_health_csv,
            iface_health_rows,
            [
                "interface_ip",
                "avg_rtt_ms",
                "throughput_bps",
                "jitter_ms",
                "loss_rate_pct",
                "instant_bitrate",
                "performance_score",
                "last_check",
            ],
        )

        write_csv(
            prediction_csv,
            prediction_rows,
            ["interface_ip", "predicted_rtt", "predicted_bitrate", "avg_jitter", "avg_loss", "blended_score", "timestamp"],
        )

        write_csv(
            scenario_csv,
            scenario_snapshot_rows,
            [
                "scenario",
                "sample_count",
                "mean_transfer_time_s",
                "variance_transfer_time_s",
                "std_transfer_time_s",
                "min_transfer_time_s",
                "max_transfer_time_s",
                "ci95_half_width_s",
                "source_payload_count",
            ],
        )

        write_csv(
            cumulative_csv,
            cumulative_rows,
            ["scenario", "file_count", "metric_column", "sample_count", "mean", "variance", "std"],
        )

        write_csv(
            scenario_significance_csv,
            scenario_significance_rows,
            [
                "scenario",
                "sample_count",
                "mean_transfer_time_s",
                "variance_transfer_time_s",
                "std_transfer_time_s",
                "ci95_half_width_s",
                "cv_pct",
                "mean_delta_32_to_final_pct",
                "variance_delta_32_to_final_pct",
                "closest_mean_std_file_count",
                "closest_mean_std_abs_gap",
                "closest_mean_std_rel_gap_pct",
                "significance_flag",
                "significance_note",
            ],
        )

        build_markdown_summary(
            summary_md,
            n_payloads=len(per_payload_rows),
            payload_rows=per_payload_rows,
            sender_csv=sender_csv,
            iface_csv=iface_health_csv,
            prediction_csv=prediction_csv,
            scenario_csv=scenario_csv,
            cumulative_csv=cumulative_csv,
            scenario_significance_csv=scenario_significance_csv,
            cumulative_rows=cumulative_rows,
            scenario_significance_rows=scenario_significance_rows,
        )

        print("Sender statistical report generated:")
        print(f" - {sender_csv}")
        print(f" - {iface_dist_csv}")
        print(f" - {iface_health_csv}")
        print(f" - {prediction_csv}")
        print(f" - {scenario_csv}")
        print(f" - {cumulative_csv}")
        print(f" - {scenario_significance_csv}")
        print(f" - {summary_md}")

        if not args.watch:
            break

        time.sleep(args.poll_interval)


if __name__ == "__main__":
    main()
