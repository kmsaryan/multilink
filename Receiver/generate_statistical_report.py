#!/usr/bin/env python3
import argparse
import csv
import hashlib
import math
import os
import sqlite3
import statistics
import time
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Optional

import config
from db_utils import (
    get_db_connection,
    init_receiver_db,
    infer_scenario_from_filename,
    store_run_statistics,
    store_scenario_statistics,
)


DEFAULT_OUTPUT_DIR = os.path.join(config.RESULTS_DIR, "statistical_reports")


def safe_mean(values: List[float]) -> Optional[float]:
    return statistics.mean(values) if values else None


def safe_stdev(values: List[float]) -> Optional[float]:
    return statistics.stdev(values) if len(values) >= 2 else None


def safe_variance(values: List[float]) -> Optional[float]:
    return statistics.variance(values) if len(values) >= 2 else None


def ci95_half_width(values: List[float]) -> Optional[float]:
    if len(values) < 2:
        return None
    sd = statistics.stdev(values)
    return 1.96 * sd / math.sqrt(len(values))


def sha256_file(file_path: str) -> Optional[str]:
    if not os.path.exists(file_path):
        return None
    digest = hashlib.sha256()
    with open(file_path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def fetch_payload_rows(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    query = """
        SELECT payload_id, filename, total_chunks, received_chunks, status,
               metadata_arrived_time, completion_time
        FROM file_map
        ORDER BY metadata_arrived_time ASC
    """
    return list(conn.execute(query).fetchall())


def fetch_interface_counts(conn: sqlite3.Connection, payload_id: str) -> Dict[str, int]:
    rows = conn.execute(
        """
        SELECT source_ip, COUNT(*) AS cnt
        FROM arrival_logs
        WHERE payload_id = ?
        GROUP BY source_ip
        """,
        (payload_id,),
    ).fetchall()
    return {row[0]: row[1] for row in rows}


def fetch_chunk_window_times(conn: sqlite3.Connection, payload_id: str) -> Dict[str, Optional[float]]:
    row = conn.execute(
        """
        SELECT MIN(arrival_time) AS first_arrival_time,
               MAX(arrival_time) AS last_arrival_time
        FROM arrival_logs
        WHERE payload_id = ?
        """,
        (payload_id,),
    ).fetchone()

    first_arrival_time = row[0] if row else None
    last_arrival_time = row[1] if row else None

    chunk_to_chunk_time_s = None
    if first_arrival_time is not None and last_arrival_time is not None and last_arrival_time >= first_arrival_time:
        chunk_to_chunk_time_s = float(last_arrival_time - first_arrival_time)

    return {
        "first_arrival_time": first_arrival_time,
        "last_arrival_time": last_arrival_time,
        "chunk_to_chunk_time_s": chunk_to_chunk_time_s,
    }


def compute_receiver_hash(received_dir: str, filename: str, payload_id: str) -> Optional[str]:
    named_path = os.path.join(received_dir, filename)
    if os.path.exists(named_path):
        return sha256_file(named_path)

    fallback = os.path.join(received_dir, f"{payload_id}.bin")
    return sha256_file(fallback)


def write_csv(path: str, rows: List[Dict[str, object]], fieldnames: List[str]) -> None:
    with open(path, "w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def fmt_number(value: Optional[float], digits: int = 3) -> str:
    if value is None:
        return "NA"
    return f"{value:.{digits}f}"


def build_markdown_summary(
    out_path: str,
    run_count: int,
    scenario_summary_rows: List[Dict[str, object]],
    per_run_csv: str,
    scenario_csv: str,
    scenario_snapshot_csv: str,
    iface_csv: str,
) -> None:
    lines = []
    lines.append("# Receiver Statistical Results Summary")
    lines.append("")
    lines.append(f"- Total runs analysed: **{run_count}**")
    lines.append(f"- Per-run metrics: `{per_run_csv}`")
    lines.append(f"- Scenario summary: `{scenario_csv}`")
    lines.append(f"- Scenario snapshot: `{scenario_snapshot_csv}`")
    lines.append(f"- Interface contribution summary: `{iface_csv}`")
    lines.append("")
    lines.append("## Scenario Summary")
    lines.append("")
    lines.append("| Scenario | Runs | Completion Rate | File Present Rate | Chunk-to-Chunk Mean ± CI95 | Chunk-to-Chunk Min/Max | File-to-File Mean ± CI95 | File-to-File Min/Max | Mean Goodput (Mbps) ± CI95 |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|")

    for row in scenario_summary_rows:
        lines.append(
            "| {scenario} | {n_runs} | {completion_rate_pct:.1f}% | {file_present_rate_pct:.1f}% | {ctc_mean} ± {ctc_ci} | {ctc_min} / {ctc_max} | {ftf_mean} ± {ftf_ci} | {ftf_min} / {ftf_max} | {g_mean} ± {g_ci} |".format(
                scenario=row["scenario"],
                n_runs=int(row["n_runs"]),
                completion_rate_pct=float(row["completion_rate_pct"]),
                file_present_rate_pct=float(row["file_present_rate_pct"]),
                ctc_mean=fmt_number(row.get("chunk_to_chunk_time_mean_s"), 3),
                ctc_ci=fmt_number(row.get("chunk_to_chunk_time_ci95_s"), 3),
                ctc_min=fmt_number(row.get("chunk_to_chunk_time_min_s"), 3),
                ctc_max=fmt_number(row.get("chunk_to_chunk_time_max_s"), 3),
                ftf_mean=fmt_number(row.get("file_to_file_time_mean_s"), 3),
                ftf_ci=fmt_number(row.get("file_to_file_time_ci95_s"), 3),
                ftf_min=fmt_number(row.get("file_to_file_time_min_s"), 3),
                ftf_max=fmt_number(row.get("file_to_file_time_max_s"), 3),
                g_mean=fmt_number(row.get("goodput_mean_mbps"), 3),
                g_ci=fmt_number(row.get("goodput_ci95_mbps"), 3),
            )
        )

    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This report is receiver-only and uses only the receiver SQLite database and received files.")
    lines.append("- `completion_rate` uses receiver `file_map` completion status and chunk counts.")
    lines.append("- `file_present_rate` checks whether the reconstructed file exists in receiver storage.")
    lines.append("- CI95 uses normal approximation: mean ± 1.96 * (std / sqrt(n)).")

    with open(out_path, "w") as handle:
        handle.write("\n".join(lines) + "\n")


def is_transfer_complete(row: sqlite3.Row) -> bool:
    total_chunks = int(row["total_chunks"] or 0)
    received_chunks = int(row["received_chunks"] or 0)
    status = row["status"] or ""
    completion_time = row["completion_time"]
    return bool(
        status == "completed"
        or (completion_time is not None)
        or (total_chunks > 0 and received_chunks >= total_chunks)
    )


def completed_signature(rows: List[sqlite3.Row]) -> tuple:
    return tuple(
        sorted(
            (
                str(row["payload_id"]),
                int(row["total_chunks"] or 0),
                int(row["received_chunks"] or 0),
                row["completion_time"],
            )
            for row in rows
        )
    )


def generate_reports_for_rows(args, payload_rows: List[sqlite3.Row], report_id: str) -> None:
    conn = get_db_connection(args.receiver_db)

    per_run_rows: List[Dict[str, object]] = []
    iface_rows: List[Dict[str, object]] = []

    for row in payload_rows:
        payload_id = row["payload_id"]
        filename = row["filename"] or ""
        total_chunks = int(row["total_chunks"] or 0)
        received_chunks = int(row["received_chunks"] or 0)
        status = row["status"] or "unknown"
        metadata_time = row["metadata_arrived_time"]
        completion_time = row["completion_time"]

        file_to_file_time_s = None
        if metadata_time and completion_time and completion_time >= metadata_time:
            file_to_file_time_s = float(completion_time - metadata_time)

        chunk_window = fetch_chunk_window_times(conn, payload_id)
        chunk_to_chunk_time_s = chunk_window["chunk_to_chunk_time_s"]

        completion_ratio = (received_chunks / total_chunks) if total_chunks > 0 else 0.0
        completed = bool(status == "completed" or (total_chunks > 0 and received_chunks >= total_chunks))

        goodput_mbps = None
        if file_to_file_time_s and file_to_file_time_s > 0:
            goodput_mbps = (received_chunks * config.CHUNK_SIZE * 8) / (file_to_file_time_s * 1_000_000)

        if args.scenario_name:
            scenario = args.scenario_name
        else:
            scenario = infer_scenario_from_filename(filename)
        receiver_sha = compute_receiver_hash(args.received_dir, filename, payload_id)
        file_present = 1 if receiver_sha else 0

        per_run_rows.append(
            {
                "payload_id": payload_id,
                "filename": filename,
                "scenario": scenario,
                "status": status,
                "completed": int(completed),
                "total_chunks": total_chunks,
                "received_chunks": received_chunks,
                "completion_ratio": completion_ratio,
                "chunk_to_chunk_time_s": chunk_to_chunk_time_s,
                "file_to_file_time_s": file_to_file_time_s,
                "goodput_mbps": goodput_mbps,
                "metadata_arrived_time": metadata_time,
                "first_arrival_time": chunk_window["first_arrival_time"],
                "last_arrival_time": chunk_window["last_arrival_time"],
                "completion_time": completion_time,
                "receiver_sha256": receiver_sha,
                "file_present": file_present,
            }
        )

        store_run_statistics(
            payload_id=payload_id,
            report_id=report_id,
            filename=filename,
            scenario=scenario,
            status=status,
            total_chunks=total_chunks,
            received_chunks=received_chunks,
            completion_ratio=completion_ratio,
            chunk_to_chunk_time_s=chunk_to_chunk_time_s,
            file_to_file_time_s=file_to_file_time_s,
            goodput_mbps=goodput_mbps,
            metadata_arrived_time=metadata_time,
            first_arrival_time=chunk_window["first_arrival_time"],
            last_arrival_time=chunk_window["last_arrival_time"],
            completion_time=completion_time,
            receiver_sha256=receiver_sha,
            file_present=file_present,
        )

        iface_counts = fetch_interface_counts(conn, payload_id)
        total_arrivals = sum(iface_counts.values())
        for source_ip, count in iface_counts.items():
            iface_rows.append(
                {
                    "payload_id": payload_id,
                    "scenario": scenario,
                    "source_ip": source_ip,
                    "chunks": count,
                    "chunk_share_pct": (count / total_arrivals * 100) if total_arrivals > 0 else 0.0,
                }
            )

    conn.close()

    by_scenario: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for run in per_run_rows:
        by_scenario[str(run["scenario"])].append(run)

    scenario_summary_rows: List[Dict[str, object]] = []
    for scenario, runs in sorted(by_scenario.items()):
        sample_count = len(runs)
        completion_vals = [float(run["completed"]) for run in runs]
        file_present_vals = [float(run["file_present"]) for run in runs]
        chunk_to_chunk_vals = [float(run["chunk_to_chunk_time_s"]) for run in runs if run["chunk_to_chunk_time_s"] is not None]
        file_to_file_vals = [float(run["file_to_file_time_s"]) for run in runs if run["file_to_file_time_s"] is not None]
        goodput_vals = [float(run["goodput_mbps"]) for run in runs if run["goodput_mbps"] is not None]

        scenario_summary_rows.append(
            {
                "scenario": scenario,
                "n_runs": sample_count,
                "sample_count": sample_count,
                "completion_rate_pct": safe_mean(completion_vals) * 100 if completion_vals else 0.0,
                "file_present_rate_pct": safe_mean(file_present_vals) * 100 if file_present_vals else 0.0,
                "chunk_to_chunk_time_mean_s": safe_mean(chunk_to_chunk_vals),
                "chunk_to_chunk_time_variance_s": safe_variance(chunk_to_chunk_vals),
                "chunk_to_chunk_time_std_s": safe_stdev(chunk_to_chunk_vals),
                "chunk_to_chunk_time_min_s": min(chunk_to_chunk_vals) if chunk_to_chunk_vals else None,
                "chunk_to_chunk_time_max_s": max(chunk_to_chunk_vals) if chunk_to_chunk_vals else None,
                "chunk_to_chunk_time_ci95_s": ci95_half_width(chunk_to_chunk_vals),
                "file_to_file_time_mean_s": safe_mean(file_to_file_vals),
                "file_to_file_time_variance_s": safe_variance(file_to_file_vals),
                "file_to_file_time_std_s": safe_stdev(file_to_file_vals),
                "file_to_file_time_min_s": min(file_to_file_vals) if file_to_file_vals else None,
                "file_to_file_time_max_s": max(file_to_file_vals) if file_to_file_vals else None,
                "file_to_file_time_ci95_s": ci95_half_width(file_to_file_vals),
                "goodput_mean_mbps": safe_mean(goodput_vals),
                "goodput_variance_mbps": safe_variance(goodput_vals),
                "goodput_std_mbps": safe_stdev(goodput_vals),
                "goodput_min_mbps": min(goodput_vals) if goodput_vals else None,
                "goodput_max_mbps": max(goodput_vals) if goodput_vals else None,
                "goodput_ci95_mbps": ci95_half_width(goodput_vals),
            }
        )

    per_run_csv = os.path.join(args.out_dir, f"per_run_metrics_{report_id}.csv")
    scenario_csv = os.path.join(args.out_dir, f"scenario_summary_{report_id}.csv")
    scenario_snapshot_csv = os.path.join(args.out_dir, f"scenario_statistics_snapshot_{report_id}.csv")
    iface_csv = os.path.join(args.out_dir, f"interface_contribution_{report_id}.csv")
    summary_md = os.path.join(args.out_dir, f"statistical_summary_{report_id}.md")

    write_csv(
        per_run_csv,
        per_run_rows,
        [
            "payload_id",
            "filename",
            "scenario",
            "status",
            "completed",
            "total_chunks",
            "received_chunks",
            "completion_ratio",
            "chunk_to_chunk_time_s",
            "file_to_file_time_s",
            "goodput_mbps",
            "metadata_arrived_time",
            "first_arrival_time",
            "last_arrival_time",
            "completion_time",
            "receiver_sha256",
            "file_present",
        ],
    )

    write_csv(
        scenario_csv,
        scenario_summary_rows,
        [
            "scenario",
            "n_runs",
            "sample_count",
            "completion_rate_pct",
            "file_present_rate_pct",
            "chunk_to_chunk_time_mean_s",
            "chunk_to_chunk_time_variance_s",
            "chunk_to_chunk_time_std_s",
            "chunk_to_chunk_time_min_s",
            "chunk_to_chunk_time_max_s",
            "chunk_to_chunk_time_ci95_s",
            "file_to_file_time_mean_s",
            "file_to_file_time_variance_s",
            "file_to_file_time_std_s",
            "file_to_file_time_min_s",
            "file_to_file_time_max_s",
            "file_to_file_time_ci95_s",
            "goodput_mean_mbps",
            "goodput_variance_mbps",
            "goodput_std_mbps",
            "goodput_min_mbps",
            "goodput_max_mbps",
            "goodput_ci95_mbps",
        ],
    )

    write_csv(
        scenario_snapshot_csv,
        scenario_summary_rows,
        [
            "scenario",
            "n_runs",
            "sample_count",
            "completion_rate_pct",
            "file_present_rate_pct",
            "chunk_to_chunk_time_mean_s",
            "chunk_to_chunk_time_variance_s",
            "chunk_to_chunk_time_std_s",
            "chunk_to_chunk_time_min_s",
            "chunk_to_chunk_time_max_s",
            "chunk_to_chunk_time_ci95_s",
            "file_to_file_time_mean_s",
            "file_to_file_time_variance_s",
            "file_to_file_time_std_s",
            "file_to_file_time_min_s",
            "file_to_file_time_max_s",
            "file_to_file_time_ci95_s",
            "goodput_mean_mbps",
            "goodput_variance_mbps",
            "goodput_std_mbps",
            "goodput_min_mbps",
            "goodput_max_mbps",
            "goodput_ci95_mbps",
        ],
    )

    store_scenario_statistics(report_id, scenario_summary_rows)

    write_csv(
        iface_csv,
        iface_rows,
        ["payload_id", "scenario", "source_ip", "chunks", "chunk_share_pct"],
    )

    build_markdown_summary(
        summary_md,
        run_count=len(per_run_rows),
        scenario_summary_rows=scenario_summary_rows,
        per_run_csv=per_run_csv,
        scenario_csv=scenario_csv,
        scenario_snapshot_csv=scenario_snapshot_csv,
        iface_csv=iface_csv,
    )

    print("Receiver statistical report generated:")
    print(f" - {per_run_csv}")
    print(f" - {scenario_csv}")
    print(f" - {scenario_snapshot_csv}")
    print(f" - {iface_csv}")
    print(f" - {summary_md}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate receiver-only multi-run statistical report from receiver DB.")
    parser.add_argument("scenario_name", nargs="?", default=None, help="Optional scenario label to apply to all runs (e.g., LOS, NLOS, SCENARIO_A). If omitted, scenario is inferred from filename.")
    parser.add_argument("--receiver-db", default=config.DB_PATH, help="Path to receiver SQLite DB.")
    parser.add_argument("--received-dir", default=config.RECEIVED_DIR, help="Path to receiver reassembled files directory.")
    parser.add_argument("--out-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory for CSV/MD summary.")
    parser.add_argument("--report-id", default=None, help="Optional identifier suffix for output files (default: timestamp).")
    parser.add_argument("--watch", action="store_true", help="Keep running and generate a new report when new completed transfers arrive.")
    parser.add_argument("--poll-interval", type=float, default=5.0, help="Polling interval in seconds when --watch is enabled.")
    args = parser.parse_args()

    if not os.path.exists(args.receiver_db):
        raise SystemExit(f"Receiver DB not found: {args.receiver_db}")

    os.makedirs(args.out_dir, exist_ok=True)
    init_receiver_db()

    if not args.watch:
        conn = get_db_connection(args.receiver_db)
        payload_rows = fetch_payload_rows(conn)
        conn.close()

        if not payload_rows:
            raise SystemExit("No payload runs found in receiver DB (file_map is empty).")

        report_id = args.report_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        generate_reports_for_rows(args, payload_rows, report_id)
        return

    poll_interval = max(0.5, float(args.poll_interval))
    print(f"Watching receiver DB for completed transfers (poll every {poll_interval:.1f}s)...")
    last_seen_signature = None

    try:
        while True:
            conn = get_db_connection(args.receiver_db)
            payload_rows = fetch_payload_rows(conn)
            conn.close()

            completed_rows = [row for row in payload_rows if is_transfer_complete(row)]
            if not completed_rows:
                print("No completed transfers yet; waiting...")
                time.sleep(poll_interval)
                continue

            current_signature = completed_signature(completed_rows)
            if current_signature != last_seen_signature:
                report_id = args.report_id or datetime.now().strftime("%Y%m%d_%H%M%S")
                generate_reports_for_rows(args, completed_rows, report_id)
                last_seen_signature = current_signature
            else:
                print("No new completed transfer changes; waiting...")

            time.sleep(poll_interval)
    except KeyboardInterrupt:
        print("\nStopped watch mode.")


if __name__ == "__main__":
    main()
