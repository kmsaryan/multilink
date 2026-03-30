#!/usr/bin/env python3
import argparse
import csv
import math
import os
import sqlite3
import statistics
from typing import Dict, List, Optional

import config


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
    lines.append("")
    lines.append("## Sender KPIs")
    lines.append("")
    lines.append(f"- Mean chunk ACK ratio: **{fmt_number(safe_mean(ack_rates), 2)}%**")
    lines.append(f"- Mean sender-side send span: **{fmt_number(safe_mean(send_spans), 3)} s**")
    lines.append(f"- Send span CI95: **±{fmt_number(ci95_half_width(send_spans), 3)} s**")
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- This report is sender-only and uses only sender DB tables (`payloads`, `chunks`, `interface_stats`, `interface_metrics_history`, `interface_predictions`).")
    lines.append("- `send_span_s` is computed from chunk `last_sent` min/max and represents sender transmission activity span, not receiver completion time.")

    with open(out_path, "w") as handle:
        handle.write("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate sender-only statistical report from sender DB.")
    parser.add_argument("--sender-db", default=config.DB_PATH, help="Path to sender SQLite DB.")
    parser.add_argument("--out-dir", default=DEFAULT_OUTPUT_DIR, help="Output directory for CSV/MD summary.")
    args = parser.parse_args()

    if not os.path.exists(args.sender_db):
        raise SystemExit(f"Sender DB not found: {args.sender_db}")

    os.makedirs(args.out_dir, exist_ok=True)

    conn = sqlite3.connect(args.sender_db)
    conn.row_factory = sqlite3.Row

    if not table_exists(conn, "payloads") or not table_exists(conn, "chunks"):
        conn.close()
        raise SystemExit("Sender DB missing required tables `payloads` or `chunks`.")

    payload_base_rows = fetch_payload_rows(conn)
    if not payload_base_rows:
        conn.close()
        raise SystemExit("No payload rows found in sender DB.")

    per_payload_rows: List[Dict[str, object]] = []
    iface_distribution_rows: List[Dict[str, object]] = []

    for row in payload_base_rows:
        payload_id = row["payload_id"]
        filename = row["filename"]
        total_chunks_declared = int(row["total_chunks"] or 0)
        status = row["status"]

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

        per_payload_rows.append(
            {
                "payload_id": payload_id,
                "filename": filename,
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
    summary_md = os.path.join(args.out_dir, "sender_statistical_summary.md")

    write_csv(
        sender_csv,
        per_payload_rows,
        [
            "payload_id",
            "filename",
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

    build_markdown_summary(
        summary_md,
        n_payloads=len(per_payload_rows),
        payload_rows=per_payload_rows,
        sender_csv=sender_csv,
        iface_csv=iface_health_csv,
        prediction_csv=prediction_csv,
    )

    print("Sender statistical report generated:")
    print(f" - {sender_csv}")
    print(f" - {iface_dist_csv}")
    print(f" - {iface_health_csv}")
    print(f" - {prediction_csv}")
    print(f" - {summary_md}")


if __name__ == "__main__":
    main()
