#!/usr/bin/env python3
import argparse
import os
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

import config
from db_utils import fetch_receiver_checkpoint_history

# Canonical CLI metric names and DB aliases (supports old + new labels)
METRIC_CHOICES = ["file_to_file", "chunk_to_chunk", "goodput"]

METRIC_DB_ALIASES = {
    "file_to_file":   ["file_to_file", "file_to_file_s"],
    "chunk_to_chunk": ["chunk_to_chunk", "chunk_to_chunk_s"],
    "goodput":        ["goodput", "goodput_mbps"],
}

METRIC_AXIS_LABELS = {
    "file_to_file":   "E2E delivery time (s)  [file_to_file_time_s]",
    "chunk_to_chunk": "Chunk-window span (s)  [chunk_to_chunk_time_s]",
    "goodput":        "Goodput (Mbps)",
}


# ──────────────────────────────────────────────────────────────
# Stability helpers  (identical logic to sender-side script)
# ──────────────────────────────────────────────────────────────

def _variance_stability_series(rows: list) -> list:
    """
    Given checkpoint rows (sorted by file_count), compute:
        delta_pct = |Var(X_k) - Var(X_{k-1})| / Var(X_{k-1}) * 100
    """
    result, prev = [], None
    for row in sorted(rows, key=lambda r: int(r["file_count"])):
        var = row.get("variance_value")
        if var is None:
            delta = None
        elif prev in (None, 0):
            delta = None
        else:
            delta = abs(var - prev) / abs(prev) * 100.0

        result.append({
            "file_count":   int(row["file_count"]),
            "sample_count": row.get("sample_count"),
            "report_count": row.get("report_count"),
            "mean":         row.get("mean_value"),
            "variance":     var,
            "std":          row.get("std_value"),
            "ci95":         row.get("ci95_value"),
            "delta_pct":    delta,
        })
        prev = var
    return result


def _find_stability_point(series: list,
                           threshold_pct: float = 5.0):
    """
    Return (stable_k, delta) at the first k where
    delta_pct ≤ threshold AND the next checkpoint also satisfies it.
    """
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


def _nearest_stability(series: list):
    best = None
    for row in series:
        d = row.get("delta_pct")
        if d is None:
            continue
        if best is None or d < best["delta_pct"]:
            best = row
    return best


# ──────────────────────────────────────────────────────────────
# 3-panel plot  (same layout as sender-side script)
# ──────────────────────────────────────────────────────────────

def _plot_stability(scenario: str, metric: str,
                    series: list, stable_k,
                    threshold_pct: float, out_dir: str) -> str:
    k         = [r["file_count"] for r in series]
    variances = [r["variance"] if r["variance"] is not None
                 else float("nan") for r in series]
    deltas    = [r["delta_pct"] if r["delta_pct"] is not None
                 else float("nan") for r in series]
    means     = np.array([r["mean"] if r["mean"] is not None
                          else float("nan") for r in series])
    stds      = np.array([r["std"]  if r["std"]  is not None
                          else float("nan") for r in series])

    y_label = METRIC_AXIS_LABELS.get(metric, metric)

    fig, axes = plt.subplots(3, 1, figsize=(10, 11), sharex=True)
    fig.suptitle(
        f"Receiver variance stability\n"
        f"Scenario: {scenario} | Metric: {metric}\n"
        f"Criterion: |Var(X_k) − Var(X_{{k−1}})| / Var(X_{{k−1}}) ≤ {threshold_pct}%",
        fontsize=11,
    )

    # ── Panel 1: cumulative variance ───────────────────────────
    ax1 = axes[0]
    ax1.plot(k, variances, "s-", color="#FF5722", linewidth=1.5,
             markersize=4, label="Var(X_k)")
    if stable_k:
        ax1.axvline(stable_k, color="green", linestyle=":",
                    linewidth=1.5, label=f"Stable at k={stable_k}")
    ax1.axvline(32, color="gray", linestyle="--", linewidth=1,
                alpha=0.6, label="k=32 reference")
    ax1.set_ylabel("Variance")
    ax1.set_title(f"Cumulative Var(X_k) vs k")
    ax1.legend(fontsize=8)
    ax1.grid(True, alpha=0.25)

    # ── Panel 2: relative variance change ──────────────────────
    ax2 = axes[1]
    ax2.plot(k, deltas, "D-", color="#9C27B0", linewidth=1.5,
             markersize=4, label="|ΔVar| / Var_{k−1} (%)")
    ax2.axhline(threshold_pct, color="red", linestyle="--",
                linewidth=1, label=f"Threshold {threshold_pct}%")
    if stable_k:
        ax2.axvline(stable_k, color="green", linestyle=":",
                    linewidth=1.5)
    ax2.axvline(32, color="gray", linestyle="--", linewidth=1, alpha=0.6)
    ax2.set_ylabel("Relative Δvariance (%)")
    ax2.set_title("Relative variance change between consecutive checkpoints")
    ax2.legend(fontsize=8)
    ax2.grid(True, alpha=0.25)

    # ── Panel 3: mean ± std ────────────────────────────────────
    ax3 = axes[2]
    ax3.plot(k, means, "o-", color="#2196F3", linewidth=1.5,
             markersize=4, label="Mean(X_k)")
    lower = means - stds
    upper = means + stds
    ax3.fill_between(k, lower, upper, alpha=0.15, color="#2196F3",
                     label="Mean ± 1 std")
    if stable_k:
        ax3.axvline(stable_k, color="green", linestyle=":",
                    linewidth=1.5, label=f"Stable at k={stable_k}")
    ax3.axvline(32, color="gray", linestyle="--", linewidth=1, alpha=0.6)
    ax3.set_xlabel("Sample size k (number of files)")
    ax3.set_ylabel(y_label)
    ax3.set_title("Cumulative mean and spread")
    ax3.legend(fontsize=8)
    ax3.grid(True, alpha=0.25)

    plt.tight_layout()

    safe_sc = "".join(c if c.isalnum() or c in ("-", "_") else "_"
                      for c in str(scenario))
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    path    = os.path.join(
        out_dir,
        f"receiver_variance_stability_{safe_sc}_{metric}_{ts}.png"
    )
    os.makedirs(out_dir, exist_ok=True)
    plt.savefig(path, dpi=150, bbox_inches="tight")
    plt.close()
    return path


# ──────────────────────────────────────────────────────────────
# Console table  (same columns as sender-side)
# ──────────────────────────────────────────────────────────────

def _print_table(scenario: str, metric: str,
                 series: list, threshold: float) -> None:
    print("\n" + "=" * 80)
    print(f"Scenario: {scenario} | Metric: {metric} | ε = {threshold}%")
    print(
        f"Criterion: |Var(X_k) − Var(X_{{k−1}})| / Var(X_{{k−1}}) < {threshold}%"
    )
    print("=" * 80)

    header = (
        f"{'k':>6}  {'samples':>7}  {'reports':>7}  "
        f"{'mean':>12}  {'variance':>14}  {'std':>12}  "
        f"{'CI95 ±':>9}  {'Δvar %':>9}"
    )
    print(header)
    print("-" * len(header))

    for row in series:
        def _f(v, w=12, d=4):
            return f"{v:{w}.{d}f}" if v is not None else f"{'NA':>{w}}"

        delta_s = (f"{row['delta_pct']:9.2f}"
                   if row["delta_pct"] is not None else f"{'—':>9}")
        print(
            f"{row['file_count']:>6}  "
            f"{int(row.get('sample_count') or 0):>7}  "
            f"{int(row.get('report_count') or 1):>7}  "
            f"{_f(row.get('mean'), 12, 4)}  "
            f"{_f(row.get('variance'), 14, 4)}  "
            f"{_f(row.get('std'), 12, 4)}  "
            f"{_f(row.get('ci95'), 9, 4)}  "
            f"{delta_s}"
        )

    stable_k, stable_d = _find_stability_point(series, threshold)
    print()
    if stable_k is not None:
        qualifier = ("within expected range (≤ 32)"
                     if stable_k <= 32
                     else "exceeds k=32 — distribution has higher natural variance")
        print(f"Variance stabilizes at k = {stable_k} "
              f"(Δvar = {stable_d:.2f}% < {threshold}%) — {qualifier}.")
    else:
        nearest = _nearest_stability(series)
        if nearest and nearest.get("delta_pct") is not None:
            print(
                f"Variance has NOT stabilized within {threshold}%. "
                f"Nearest point: k={nearest['file_count']} "
                f"(Δvar={nearest['delta_pct']:.2f}%)."
            )
            print("Consider collecting more samples or raising --threshold.")
        else:
            print(f"No valid Δvariance values found for metric={metric}.")


# ──────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Receiver variance stability — identical logic to sender-side.\n"
            "Default metric: file_to_file (true E2E delivery time)."
        )
    )
    parser.add_argument("--receiver-db", default=config.DB_PATH)
    parser.add_argument(
        "--scenario", default=None,
        help="Filter by scenario (e.g. los, nlos). "
             "Omit to process all scenarios.",
    )
    parser.add_argument(
        "--metric", default="file_to_file",
        choices=["all"] + METRIC_CHOICES,
        help=(
            "Metric to analyse.\n"
            "  file_to_file   — PRIMARY: true E2E delivery time (default)\n"
            "  chunk_to_chunk — chunk arrival window\n"
            "  goodput        — derived throughput\n"
            "  all              — produce plots for all three"
        ),
    )
    parser.add_argument("--threshold", type=float, default=5.0,
                        help="Δvariance threshold %% to declare stability.")
    parser.add_argument(
        "--out-dir",
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "modeling_reports", "statistical_reports",
        ),
    )
    args = parser.parse_args()

    if not os.path.exists(args.receiver_db):
        sys.exit(f"Receiver DB not found: {args.receiver_db}")

    metrics = METRIC_CHOICES if args.metric == "all" else [args.metric]

    all_rows = fetch_receiver_checkpoint_history(
        db_path=args.receiver_db, scenario=args.scenario)
    if not all_rows:
        sys.exit(
            "No checkpoint history found.\n"
            "Run generate_receiver_statistical_report.py first to populate "
            "checkpoint_statistics_history."
        )

    scenarios = sorted({str(r.get("scenario") or "unknown")
                        for r in all_rows})
    generated = 0

    for scenario in scenarios:
        for metric in metrics:
            metric_aliases = METRIC_DB_ALIASES.get(metric, [metric])
            available_metric_labels = {
                str(r.get("metric"))
                for r in all_rows
                if str(r.get("scenario") or "unknown") == scenario
            }
            selected_metric_label = next(
                (label for label in metric_aliases if label in available_metric_labels),
                None,
            )

            if selected_metric_label is None:
                print(f"[skip] No data for scenario={scenario}, "
                      f"metric={metric}")
                continue

            rows = [
                r for r in all_rows
                if str(r.get("scenario") or "unknown") == scenario
                and str(r.get("metric")) == selected_metric_label
            ]
            if not rows:
                print(f"[skip] No data for scenario={scenario}, "
                      f"metric={metric}")
                continue

            series   = _variance_stability_series(rows)
            _print_table(scenario, metric, series, args.threshold)

            stable_k, _ = _find_stability_point(series, args.threshold)
            path = _plot_stability(scenario, metric, series,
                                   stable_k, args.threshold, args.out_dir)
            print(f"Plot saved → {path}")
            generated += 1

    print(f"\n{generated} variance stability plot(s) generated.")


if __name__ == "__main__":
    main()
