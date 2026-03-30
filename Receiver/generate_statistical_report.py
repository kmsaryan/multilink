#!/usr/bin/env python3
import argparse
import csv
import hashlib
import math
import os
import sqlite3
import statistics
from collections import defaultdict
from typing import Dict, List, Optional

import config


DEFAULT_OUTPUT_DIR = os.path.join(config.RESULTS_DIR, "statistical_reports")


def safe_mean(values: List[float]) -> Optional[float]:
    return statistics.mean(values) if values else None


def safe_stdev(values: List[float]) -> Optional[float]:
    return statistics.stdev(values) if len(values) >= 2 else None


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


def infer_scenario(filename: str) -> str:
    lowered = filename.lower()
    if "los" in lowered and "nlos" not in lowered:
        return "LOS"
    if "nlos" in lowered:
        return "NLOS"
    if "__" in filename:
        return filename.split("__", 1)[0]
    return "UNSPECIFIED"


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
    iface_csv: str,
) -> None:
    lines = []
    lines.append("# Receiver Statistical Results Summary")
    lines.append("")
    lines.append(f"- Total runs analysed: **{run_count}**")
    lines.append(f"- Per-run metrics: `{per_run_csv}`")
    lines.append(f"- Scenario summary: `{scenario_csv}`")
    lines.append(f"- Interface contribution summary: `{iface_csv}`")
    lines.append("")
    lines.append("## Scenario Summary")
    lines.append("")
    lines.append("| Scenario | Runs | Completion Rate | File Present Rate | Mean Transfer Time (s) ± CI95 | Mean Goodput (Mbps) ± CI95 |")
    lines.append("|---|---:|---:|---:|---:|---:|")

    for row in scenario_summary_rows:
        lines.append(
            "| {scenario} | {n_runs} | {completion_rate_pct:.1f}% | {file_present_rate_pct:.1f}% | {t_mean} ± {t_ci} | {g_mean} ± {g_ci} |".format(
                scenario=row["scenario"],
                n_runs=int(row["n_runs"]),
                completion_rate_pct=float(row["completion_rate_pct"]),
                file_present_rate_pct=float(row["file_present_rate_pct"]),
                t_mean=fmt_number(row.get("transfer_time_mean_s"), 3),
                t_ci=fmt_number(row.get("transfer_time_ci95_s"), 3),
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate receiver-only multi-run statistical report from receiver DB.")
    parser.add_argument("--receiver-db", default=config.DB_PATH, help="Path to receiver SQLite DB.")
    parser.add_argument("--received-dir", default=config.RECEIVED_DIR, help="Path to receiver reassembled files directory.")
    parser.add_argument("--out-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory for CSV/MD summary.")
    args = parser.parse_args()

    if not os.path.exists(args.receiver_db):
        raise SystemExit(f"Receiver DB not found: {args.receiver_db}")

    os.makedirs(args.out_dir, exist_ok=True)

    conn = sqlite3.connect(args.receiver_db)
    payload_rows = fetch_payload_rows(conn)

    if not payload_rows:
        conn.close()
        raise SystemExit("No payload runs found in receiver DB (file_map is empty).")

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

        transfer_time_s = None
        if metadata_time and completion_time and completion_time >= metadata_time:
            transfer_time_s = float(completion_time - metadata_time)

        completion_ratio = (received_chunks / total_chunks) if total_chunks > 0 else 0.0
        completed = bool(status == "completed" or (total_chunks > 0 and received_chunks >= total_chunks))

        goodput_mbps = None
        if transfer_time_s and transfer_time_s > 0:
            goodput_mbps = (received_chunks * config.CHUNK_SIZE * 8) / (transfer_time_s * 1_000_000)

        scenario = infer_scenario(filename)
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
                "transfer_time_s": transfer_time_s,
                "goodput_mbps": goodput_mbps,
                "metadata_arrived_time": metadata_time,
                "completion_time": completion_time,
                "receiver_sha256": receiver_sha,
                "file_present": file_present,
            }
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
        completion_vals = [float(run["completed"]) for run in runs]
        file_present_vals = [float(run["file_present"]) for run in runs]
        time_vals = [float(run["transfer_time_s"]) for run in runs if run["transfer_time_s"] is not None]
        goodput_vals = [float(run["goodput_mbps"]) for run in runs if run["goodput_mbps"] is not None]

        scenario_summary_rows.append(
            {
                "scenario": scenario,
                "n_runs": len(runs),
                "completion_rate_pct": safe_mean(completion_vals) * 100 if completion_vals else 0.0,
                "file_present_rate_pct": safe_mean(file_present_vals) * 100 if file_present_vals else 0.0,
                "transfer_time_mean_s": safe_mean(time_vals),
                "transfer_time_std_s": safe_stdev(time_vals),
                "transfer_time_ci95_s": ci95_half_width(time_vals),
                "goodput_mean_mbps": safe_mean(goodput_vals),
                "goodput_std_mbps": safe_stdev(goodput_vals),
                "goodput_ci95_mbps": ci95_half_width(goodput_vals),
            }
        )

    per_run_csv = os.path.join(args.out_dir, "per_run_metrics.csv")
    scenario_csv = os.path.join(args.out_dir, "scenario_summary.csv")
    iface_csv = os.path.join(args.out_dir, "interface_contribution.csv")
    summary_md = os.path.join(args.out_dir, "statistical_summary.md")

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
            "transfer_time_s",
            "goodput_mbps",
            "metadata_arrived_time",
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
            "completion_rate_pct",
            "file_present_rate_pct",
            "transfer_time_mean_s",
            "transfer_time_std_s",
            "transfer_time_ci95_s",
            "goodput_mean_mbps",
            "goodput_std_mbps",
            "goodput_ci95_mbps",
        ],
    )

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
        iface_csv=iface_csv,
    )

    print("Receiver statistical report generated:")
    print(f" - {per_run_csv}")
    print(f" - {scenario_csv}")
    print(f" - {iface_csv}")
    print(f" - {summary_md}")


if __name__ == "__main__":
    main()
