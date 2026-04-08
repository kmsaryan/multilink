#!/usr/bin/env python3
import argparse
import csv
import hashlib
import math
import os
import re
import sqlite3
import statistics
import time
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple

import config
from db_utils import (
    get_db_connection,
    init_receiver_db,
    infer_scenario_from_filename,
    store_receiver_checkpoint_statistics,
    store_run_statistics,
    store_scenario_statistics,
)

DEFAULT_OUTPUT_DIR = os.path.join(config.RESULTS_DIR, "statistical_reports")

# ──────────────────────────────────────────────────────────────
# Metric specs: (column in per_run dict, label stored in DB)
# The DB label is what find_receiver_significance.py filters on.
# ──────────────────────────────────────────────────────────────
METRIC_SPECS: List[Tuple[str, str]] = [
    ("file_to_file_time_s",   "file_to_file"),   # PRIMARY
    ("chunk_to_chunk_time_s", "chunk_to_chunk"),  # SECONDARY
    ("goodput_mbps",          "goodput_mbps"),       # DERIVED
]


# ──────────────────────────────────────────────────────────────
# Pure-math helpers
# ──────────────────────────────────────────────────────────────

def _safe_mean(v: List[float]) -> Optional[float]:
    return statistics.mean(v) if v else None

def _safe_stdev(v: List[float]) -> Optional[float]:
    return statistics.stdev(v) if len(v) >= 2 else None

def _safe_variance(v: List[float]) -> Optional[float]:
    return statistics.variance(v) if len(v) >= 2 else None

def _ci95(v: List[float]) -> Optional[float]:
    if len(v) < 2:
        return None
    return 1.96 * statistics.stdev(v) / math.sqrt(len(v))

def _fmt(value: Optional[float], d: int = 3) -> str:
    return "NA" if value is None else f"{value:.{d}f}"


# ──────────────────────────────────────────────────────────────
# Numeric sort key (mirrors sender side)
# ──────────────────────────────────────────────────────────────

def _run_key(filename: str) -> Tuple[int, str]:
    m = re.search(r"(\d+)", filename or "")
    return (int(m.group(1)), filename or "") if m else (10 ** 9, filename or "")


# ──────────────────────────────────────────────────────────────
# Integrity helpers
# ──────────────────────────────────────────────────────────────

def _sha256_file(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _receiver_sha256(received_dir: str, filename: str,
                     payload_id: str) -> Optional[str]:
    named = os.path.join(received_dir, filename)
    if os.path.exists(named):
        return _sha256_file(named)
    return _sha256_file(os.path.join(received_dir, f"{payload_id}.bin"))


# ──────────────────────────────────────────────────────────────
# DB read helpers
# ──────────────────────────────────────────────────────────────

def _fetch_payload_rows(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    conn.row_factory = sqlite3.Row
    return list(conn.execute(
        """SELECT payload_id, filename, total_chunks, received_chunks,
                  status, metadata_arrived_time, completion_time
           FROM   file_map
           ORDER  BY metadata_arrived_time ASC"""
    ).fetchall())


def _fetch_chunk_window(conn: sqlite3.Connection,
                        payload_id: str) -> Dict[str, Optional[float]]:
    row = conn.execute(
        """SELECT MIN(arrival_time) AS first, MAX(arrival_time) AS last
           FROM   arrival_logs WHERE payload_id = ?""",
        (payload_id,),
    ).fetchone()
    first = row[0] if row else None
    last  = row[1] if row else None
    span  = float(last - first) if (first and last and last >= first) else None
    return {"first_arrival_time": first,
            "last_arrival_time":  last,
            "chunk_to_chunk_time_s": span}


def _fetch_interface_arrivals(conn: sqlite3.Connection,
                               payload_id: str) -> Dict[str, int]:
    rows = conn.execute(
        """SELECT source_ip, COUNT(*) AS cnt
           FROM   arrival_logs WHERE payload_id = ? GROUP BY source_ip""",
        (payload_id,),
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def _is_complete(row: sqlite3.Row) -> bool:
    total = int(row["total_chunks"] or 0)
    recv  = int(row["received_chunks"] or 0)
    return bool(
        row["status"] == "completed"
        or row["completion_time"] is not None
        or (total > 0 and recv >= total)
    )


def _completed_signature(rows: List[sqlite3.Row]) -> tuple:
    return tuple(sorted(
        (str(r["payload_id"]), int(r["total_chunks"] or 0),
         int(r["received_chunks"] or 0), r["completion_time"])
        for r in rows
    ))


# ──────────────────────────────────────────────────────────────
# Checkpoint builder — mirrors sender-side build_cumulative_rows
# ──────────────────────────────────────────────────────────────

def _select_checkpoints(n: int, step: int,
                         max_files: Optional[int]) -> List[int]:
    step = max(1, step)
    cap  = n if max_files is None else max(1, min(n, max_files))
    pts  = list(range(step, cap + 1, step))
    if cap not in pts:
        pts.append(cap)
    if n > cap and n not in pts:
        pts.append(n)
    return sorted(set(pts))


def _build_checkpoint_rows(
    payload_rows: Sequence[Dict],
    checkpoint_step: int,
    max_files: Optional[int],
    scenario: str,
) -> List[Dict]:
    """
    Returns one row per (checkpoint_k, metric) with:
        scenario, file_count, metric, sample_count, mean, variance, std, ci95
    Keys match what store_receiver_checkpoint_statistics() expects.
    """
    ordered = sorted(payload_rows, key=lambda r: _run_key(str(r.get("filename", ""))))
    checkpoints = _select_checkpoints(len(ordered), checkpoint_step, max_files)

    output: List[Dict] = []
    for k in checkpoints:
        subset = ordered[:k]
        for col_key, metric_label in METRIC_SPECS:
            vals = [float(r[col_key]) for r in subset
                    if r.get(col_key) is not None]
            output.append({
                "scenario":     scenario,
                "file_count":   k,
                "metric":       metric_label,   # ← key must be "metric" for DB
                "sample_count": len(vals),
                "mean":         _safe_mean(vals),
                "variance":     _safe_variance(vals),
                "std":          _safe_stdev(vals),
                "ci95":         _ci95(vals),    # ← stored as ci95_value in DB
            })
    return output


# ──────────────────────────────────────────────────────────────
# Variance stability (same logic as sender's find_significance)
# ──────────────────────────────────────────────────────────────

def _stability_series(ckpt_rows: List[Dict],
                      metric_label: str) -> List[Dict]:
    """Extract stability series for one metric from checkpoint rows."""
    filtered = sorted(
        [r for r in ckpt_rows if r["metric"] == metric_label
         and r.get("variance") is not None],
        key=lambda r: int(r["file_count"]),
    )
    result, prev = [], None
    for r in filtered:
        var   = float(r["variance"])
        delta = (abs(var - prev) / prev * 100.0
                 if (prev not in (None, 0)) else None)
        result.append({"file_count": int(r["file_count"]),
                        "variance":   var,
                        "delta_pct":  delta})
        prev = var
    return result


def _find_stability_point(
    series: List[Dict], threshold_pct: float = 5.0
) -> Tuple[Optional[int], Optional[float]]:
    for i, row in enumerate(series):
        d = row.get("delta_pct")
        if d is None or d > threshold_pct:
            continue
        if i + 1 < len(series):
            nd = series[i + 1].get("delta_pct")
            if nd is not None and nd <= threshold_pct:
                return int(row["file_count"]), float(d)
        else:
            return int(row["file_count"]), float(d)
    return None, None


# ──────────────────────────────────────────────────────────────
# Significance summary (one row per scenario, mirrors sender)
# ──────────────────────────────────────────────────────────────

def _build_significance_rows(
    per_run: Sequence[Dict],
    checkpoint_step: int,
    max_files: Optional[int],
) -> List[Dict]:
    groups: Dict[str, List[Dict]] = defaultdict(list)
    for r in per_run:
        groups[str(r.get("scenario") or "unknown")].append(r)

    result = []
    for scenario, rows in sorted(groups.items()):
        e2e = [float(r["file_to_file_time_s"]) for r in rows
               if r.get("file_to_file_time_s") is not None]
        if not e2e:
            continue

        ckpt = _build_checkpoint_rows(rows, checkpoint_step,
                                       max_files, scenario)
        series = _stability_series(ckpt, "file_to_file_s")
        stable_k, stable_d = _find_stability_point(series, 5.0)

        mean_v = _safe_mean(e2e)
        std_v  = _safe_stdev(e2e)
        cv     = (std_v / mean_v * 100.0
                  if std_v and mean_v else None)

        if stable_k:
            flag = "stable"
            note = (f"Var(X_k) stabilizes at k={stable_k} "
                    f"(relative change < 5% for two consecutive checkpoints)")
        elif len(e2e) >= 32:
            flag = "sufficient_32"
            note = "32+ samples — CLT approximations valid"
        else:
            flag = "insufficient"
            note = (f"Only {len(e2e)} samples; variance has not stabilized. "
                    "Collect more runs.")

        result.append({
            "scenario":             scenario,
            "sample_count":         len(e2e),
            "e2e_mean_s":           mean_v,
            "e2e_variance_s":       _safe_variance(e2e),
            "e2e_std_s":            std_v,
            "e2e_ci95_s":           _ci95(e2e),
            "cv_pct":               cv,
            "stable_k":             stable_k,
            "stability_delta_pct":  stable_d,
            "significance_flag":    flag,
            "significance_note":    note,
        })
    return result


# ──────────────────────────────────────────────────────────────
# CSV writer
# ──────────────────────────────────────────────────────────────

def _write_csv(path: str, rows: List[Dict],
               fieldnames: List[str]) -> None:
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames,
                           extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


# ──────────────────────────────────────────────────────────────
# Main report
# ──────────────────────────────────────────────────────────────

def generate_report(args, payload_rows: List[sqlite3.Row],
                    report_id: str) -> None:
    conn = get_db_connection(args.receiver_db)

    per_run:       List[Dict] = []
    iface_arrivals: List[Dict] = []

    for row in payload_rows:
        pid        = row["payload_id"]
        filename   = row["filename"] or ""
        total_ch   = int(row["total_chunks"] or 0)
        recv_ch    = int(row["received_chunks"] or 0)
        status     = row["status"] or "unknown"
        meta_time  = row["metadata_arrived_time"]
        comp_time  = row["completion_time"]

        # ── Primary metric: true E2E delivery time ──────────────
        file_to_file = (
            float(comp_time - meta_time)
            if (meta_time and comp_time and comp_time >= meta_time)
            else None
        )

        # ── Secondary: chunk arrival window ──────────────────────
        window = _fetch_chunk_window(conn, pid)

        # ── Derived: goodput from true E2E time ──────────────────
        goodput = None
        if file_to_file and file_to_file > 0:
            goodput = (recv_ch * config.CHUNK_SIZE * 8
                       / (file_to_file * 1_000_000))

        completion_ratio = recv_ch / total_ch if total_ch > 0 else 0.0

        scenario = (args.scenario_name
                    if args.scenario_name
                    else infer_scenario_from_filename(filename))

        sha       = _receiver_sha256(args.received_dir, filename, pid)
        file_pres = 1 if sha else 0

        run_dict = {
            "payload_id":           pid,
            "filename":             filename,
            "scenario":             scenario,
            "status":               status,
            "total_chunks":         total_ch,
            "received_chunks":      recv_ch,
            "completion_ratio":     completion_ratio,
            "file_to_file_time_s":  file_to_file,       # PRIMARY
            "chunk_to_chunk_time_s": window["chunk_to_chunk_time_s"],
            "goodput_mbps":         goodput,
            "metadata_arrived_time": meta_time,
            "first_arrival_time":   window["first_arrival_time"],
            "last_arrival_time":    window["last_arrival_time"],
            "completion_time":      comp_time,
            "receiver_sha256":      sha,
            "file_present":         file_pres,
        }
        per_run.append(run_dict)

        # Upsert into run_statistics (same signature as DB function)
        store_run_statistics(
            payload_id=pid,
            report_id=report_id,
            filename=filename,
            scenario=scenario,
            status=status,
            total_chunks=total_ch,
            received_chunks=recv_ch,
            completion_ratio=completion_ratio,
            chunk_to_chunk_time_s=window["chunk_to_chunk_time_s"],
            file_to_file_time_s=file_to_file,
            goodput_mbps=goodput,
            metadata_arrived_time=meta_time,
            first_arrival_time=window["first_arrival_time"],
            last_arrival_time=window["last_arrival_time"],
            completion_time=comp_time,
            receiver_sha256=sha,
            file_present=file_pres,
        )

        # Per-interface arrival counts (receiver's perspective)
        arrivals    = _fetch_interface_arrivals(conn, pid)
        total_arr   = sum(arrivals.values())
        for ip, count in arrivals.items():
            iface_arrivals.append({
                "payload_id":        pid,
                "scenario":          scenario,
                "source_ip":         ip,
                "chunks_arrived":    count,
                "arrival_share_pct": (count / total_arr * 100
                                      if total_arr > 0 else 0.0),
            })

    conn.close()

    # ── Group by scenario ─────────────────────────────────────
    groups: Dict[str, List[Dict]] = defaultdict(list)
    for r in per_run:
        groups[str(r["scenario"])].append(r)

    # ── Checkpoint rows (written to DB + CSV) ─────────────────
    all_ckpt: List[Dict] = []
    for sc, sc_rows in sorted(groups.items()):
        ckpt = _build_checkpoint_rows(
            sc_rows,
            checkpoint_step=args.checkpoint_step,
            max_files=args.max_files,
            scenario=sc,
        )
        all_ckpt.extend(ckpt)
        store_receiver_checkpoint_statistics(
            db_path=args.receiver_db,
            report_id=report_id,
            scenario=sc,
            rows=ckpt,
        )

    # ── Scenario-level summary ────────────────────────────────
    # Dict keys MUST match what store_scenario_statistics() reads.
    scenario_summary: List[Dict] = []
    for sc, rows in sorted(groups.items()):
        e2e_v  = [float(r["file_to_file_time_s"])   for r in rows
                  if r.get("file_to_file_time_s") is not None]
        c2c_v  = [float(r["chunk_to_chunk_time_s"]) for r in rows
                  if r.get("chunk_to_chunk_time_s") is not None]
        gp_v   = [float(r["goodput_mbps"])           for r in rows
                  if r.get("goodput_mbps") is not None]
        comp_v = [float(r["completion_ratio"] >= 1.0) for r in rows]
        pres_v = [float(r["file_present"])            for r in rows]

        scenario_summary.append({
            # ── identity ──────────────────────────────────────
            "scenario":    sc,
            "n_runs":      len(rows),
            "sample_count": len(rows),
            # ── integrity ─────────────────────────────────────
            "completion_rate_pct":       (_safe_mean(comp_v) or 0.0) * 100,
            "file_present_rate_pct":     (_safe_mean(pres_v) or 0.0) * 100,
            # ── chunk-to-chunk (secondary) ────────────────────
            "chunk_to_chunk_time_mean_s":     _safe_mean(c2c_v),
            "chunk_to_chunk_time_variance_s": _safe_variance(c2c_v),
            "chunk_to_chunk_time_std_s":      _safe_stdev(c2c_v),
            "chunk_to_chunk_time_min_s":      min(c2c_v) if c2c_v else None,
            "chunk_to_chunk_time_max_s":      max(c2c_v) if c2c_v else None,
            "chunk_to_chunk_time_ci95_s":     _ci95(c2c_v),
            # ── file-to-file (PRIMARY) ────────────────────────
            "file_to_file_time_mean_s":     _safe_mean(e2e_v),
            "file_to_file_time_variance_s": _safe_variance(e2e_v),
            "file_to_file_time_std_s":      _safe_stdev(e2e_v),
            "file_to_file_time_min_s":      min(e2e_v) if e2e_v else None,
            "file_to_file_time_max_s":      max(e2e_v) if e2e_v else None,
            "file_to_file_time_ci95_s":     _ci95(e2e_v),
            # ── goodput (derived) ─────────────────────────────
            "goodput_mean_mbps":     _safe_mean(gp_v),
            "goodput_variance_mbps": _safe_variance(gp_v),
            "goodput_std_mbps":      _safe_stdev(gp_v),
            "goodput_min_mbps":      min(gp_v) if gp_v else None,
            "goodput_max_mbps":      max(gp_v) if gp_v else None,
            "goodput_ci95_mbps":     _ci95(gp_v),
        })

    store_scenario_statistics(report_id, scenario_summary)

    # ── Significance summary (console + CSV) ──────────────────
    significance = _build_significance_rows(
        per_run, args.checkpoint_step, args.max_files)

    # ── Write CSVs ────────────────────────────────────────────
    os.makedirs(args.out_dir, exist_ok=True)

    per_run_csv   = os.path.join(args.out_dir,
                                 f"receiver_per_run_{report_id}.csv")
    scenario_csv  = os.path.join(args.out_dir,
                                 f"receiver_scenario_summary_{report_id}.csv")
    ckpt_csv      = os.path.join(args.out_dir,
                                 f"receiver_checkpoints_{report_id}.csv")
    iface_csv     = os.path.join(args.out_dir,
                                 f"receiver_interface_arrivals_{report_id}.csv")
    sig_csv       = os.path.join(args.out_dir,
                                 f"receiver_significance_{report_id}.csv")

    _write_csv(per_run_csv, per_run, [
        "payload_id", "filename", "scenario", "status",
        "total_chunks", "received_chunks", "completion_ratio",
        "file_to_file_time_s", "chunk_to_chunk_time_s", "goodput_mbps",
        "metadata_arrived_time", "first_arrival_time",
        "last_arrival_time", "completion_time",
        "receiver_sha256", "file_present",
    ])

    _write_csv(scenario_csv, scenario_summary, [
        "scenario", "n_runs", "sample_count",
        "completion_rate_pct", "file_present_rate_pct",
        "file_to_file_time_mean_s", "file_to_file_time_variance_s",
        "file_to_file_time_std_s", "file_to_file_time_min_s",
        "file_to_file_time_max_s", "file_to_file_time_ci95_s",
        "chunk_to_chunk_time_mean_s", "chunk_to_chunk_time_variance_s",
        "chunk_to_chunk_time_std_s", "chunk_to_chunk_time_min_s",
        "chunk_to_chunk_time_max_s", "chunk_to_chunk_time_ci95_s",
        "goodput_mean_mbps", "goodput_variance_mbps",
        "goodput_std_mbps", "goodput_min_mbps",
        "goodput_max_mbps", "goodput_ci95_mbps",
    ])

    _write_csv(ckpt_csv, all_ckpt, [
        "scenario", "file_count", "metric",
        "sample_count", "mean", "variance", "std", "ci95",
    ])

    _write_csv(iface_csv, iface_arrivals, [
        "payload_id", "scenario", "source_ip",
        "chunks_arrived", "arrival_share_pct",
    ])

    _write_csv(sig_csv, significance, [
        "scenario", "sample_count",
        "e2e_mean_s", "e2e_variance_s", "e2e_std_s", "e2e_ci95_s",
        "cv_pct", "stable_k", "stability_delta_pct",
        "significance_flag", "significance_note",
    ])

    # ── Console summary ───────────────────────────────────────
    _print_summary(scenario_summary, significance, report_id, args.out_dir)


def _print_summary(scenario_rows, sig_rows, report_id, out_dir):
    W = 74
    print("=" * W)
    print(f"RECEIVER STATISTICAL REPORT  —  {report_id}")
    print("=" * W)

    print("\nSCENARIO STATISTICS  (primary metric: file_to_file_time_s)")
    print(f"  {'Scenario':<16} {'n':>4}  {'E2E mean (s)':>13}  "
          f"{'Std (s)':>10}  {'CI95 ±':>9}  {'Goodput Mbps':>13}")
    print("  " + "-" * (W - 2))
    for r in scenario_rows:
        print(
            f"  {r['scenario']:<16} {r['n_runs']:>4}  "
            f"{_fmt(r['file_to_file_time_mean_s']):>13}  "
            f"{_fmt(r['file_to_file_time_std_s']):>10}  "
            f"{_fmt(r['file_to_file_time_ci95_s']):>9}  "
            f"{_fmt(r['goodput_mean_mbps']):>13}"
        )

    print("\nINTEGRITY  (completion_ratio = 1.0  AND  SHA-256 on disk)")
    print(f"  {'Scenario':<16} {'n':>4}  {'Complete':>10}  {'SHA OK':>10}")
    print("  " + "-" * 44)
    for r in scenario_rows:
        n = r["n_runs"]
        comp_n = round(r["completion_rate_pct"] / 100 * n)
        sha_n  = round(r["file_present_rate_pct"] / 100 * n)
        print(f"  {r['scenario']:<16} {n:>4}  "
              f"{comp_n}/{n} ({r['completion_rate_pct']:.1f}%)   "
              f"{sha_n}/{n}  ({r['file_present_rate_pct']:.1f}%)")

    print("\nVARIANCE STABILITY  (ε = 5%, metric: file_to_file_s)")
    print(f"  {'Scenario':<16} {'n':>4}  {'Stable k':>9}  {'Flag':<22}  Note")
    print("  " + "-" * (W - 2))
    for r in sig_rows:
        sk = str(r["stable_k"]) if r["stable_k"] else "N/A"
        print(f"  {r['scenario']:<16} {r['sample_count']:>4}  "
              f"{sk:>9}  {r['significance_flag']:<22}  "
              f"{r['significance_note'][:38]}")

    print(f"\nOutput directory: {out_dir}")
    print("=" * W)


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Receiver statistical report — mirrors sender-side analysis.\n"
            "Primary metric: file_to_file_time_s (true E2E delivery time)."
        )
    )
    parser.add_argument(
        "scenario_name", nargs="?", default=None,
        help="Scenario label (e.g. los, nlos). "
             "If omitted, inferred from filename.",
    )
    parser.add_argument("--receiver-db",     default=config.DB_PATH)
    parser.add_argument("--received-dir",    default=config.RECEIVED_DIR)
    parser.add_argument("--out-dir",         default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--checkpoint-step", type=int, default=2,
                        help="Checkpoint increment (number of files).")
    parser.add_argument("--max-files",       type=int, default=50,
                        help="Maximum checkpoint file-count to evaluate.")
    parser.add_argument("--report-id",       default=None,
                        help="Optional suffix for output file names.")
    parser.add_argument("--watch",           action="store_true",
                        help="Re-run whenever new completed transfers arrive.")
    parser.add_argument("--poll-interval",   type=float, default=5.0)
    args = parser.parse_args()

    if not os.path.exists(args.receiver_db):
        raise SystemExit(f"Receiver DB not found: {args.receiver_db}")

    os.makedirs(args.out_dir, exist_ok=True)
    init_receiver_db()

    def _report_id():
        return args.report_id or datetime.now().strftime("%Y%m%d_%H%M%S")

    if not args.watch:
        conn = get_db_connection(args.receiver_db)
        rows = _fetch_payload_rows(conn)
        conn.close()
        if not rows:
            raise SystemExit("No payload rows in receiver DB (file_map empty).")
        generate_report(args, rows, _report_id())
        return

    print(f"Watching receiver DB (poll every {args.poll_interval:.1f}s) …")
    last_sig = None
    try:
        while True:
            conn = get_db_connection(args.receiver_db)
            rows = _fetch_payload_rows(conn)
            conn.close()
            done = [r for r in rows if _is_complete(r)]
            if not done:
                print("No completed transfers yet; waiting …")
                time.sleep(args.poll_interval)
                continue
            sig = _completed_signature(done)
            if sig != last_sig:
                generate_report(args, done, _report_id())
                last_sig = sig
            else:
                print("No new completed transfers; waiting …")
            time.sleep(args.poll_interval)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()