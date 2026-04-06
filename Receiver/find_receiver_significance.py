#!/usr/bin/env python3

import argparse
import os
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import config
from db_utils import fetch_receiver_checkpoint_history


METRIC_CHOICES = ["chunk_to_chunk", "file_to_file", "goodput"]


def variance_stability_series(rows):
    series = []
    prev_variance = None
    for row in sorted(rows, key=lambda r: int(r["file_count"])):
        variance = row.get("variance_value")
        if variance is None or prev_variance in (None, 0):
            delta_pct = None
        else:
            delta_pct = abs(variance - prev_variance) / abs(prev_variance) * 100.0

        series.append(
            {
                "file_count": int(row["file_count"]),
                "sample_count": row.get("sample_count"),
                "report_count": row.get("report_count"),
                "mean": row.get("mean_value"),
                "variance": variance,
                "std": row.get("std_value"),
                "delta_pct": delta_pct,
            }
        )
        prev_variance = variance
    return series


def find_stability_point(series, threshold_pct=5.0):
    for idx, row in enumerate(series):
        delta = row.get("delta_pct")
        if delta is None or delta > threshold_pct:
            continue

        if idx + 1 < len(series):
            next_delta = series[idx + 1].get("delta_pct")
            if next_delta is not None and next_delta <= threshold_pct:
                return row
        else:
            return row
    return None


def find_nearest_stability(series):
    best = None
    for row in series:
        delta = row.get("delta_pct")
        if delta is None:
            continue
        if best is None or delta < best["delta_pct"]:
            best = row
    return best


def print_table(scenario, metric, series, threshold):
    print("\n" + "=" * 78)
    print(f"Scenario: {scenario} | Metric: {metric} | threshold={threshold}%")
    print("=" * 78)
    header = (
        f"{'k':>5}  {'sample':>6}  {'reports':>7}  {'mean':>12}  "
        f"{'variance':>12}  {'std':>12}  {'Δvar %':>8}"
    )
    print(header)
    print("-" * len(header))

    for row in series:
        mean_str = f"{row['mean']:.4f}" if row.get("mean") is not None else "NA"
        var_str = f"{row['variance']:.4f}" if row.get("variance") is not None else "NA"
        std_str = f"{row['std']:.4f}" if row.get("std") is not None else "NA"
        delta_str = f"{row['delta_pct']:.2f}" if row.get("delta_pct") is not None else "—"
        print(
            f"{row['file_count']:>5}  {int(row.get('sample_count') or 0):>6}  "
            f"{int(row.get('report_count') or 0):>7}  {mean_str:>12}  "
            f"{var_str:>12}  {std_str:>12}  {delta_str:>8}"
        )

    stable = find_stability_point(series, threshold_pct=threshold)
    if stable:
        print(
            f"\nVariance stability reached at k={stable['file_count']} "
            f"(Δvar={stable['delta_pct']:.2f}% <= {threshold}%)."
        )
    else:
        nearest = find_nearest_stability(series)
        if nearest:
            print(
                f"\nNo stability within {threshold}%. "
                f"Closest point: k={nearest['file_count']} "
                f"(Δvar={nearest['delta_pct']:.2f}%)."
            )
        else:
            print(f"\nNo valid variance deltas available for metric={metric}.")


def plot_series(scenario, metric, series, threshold, out_dir):
    os.makedirs(out_dir, exist_ok=True)

    k = [row["file_count"] for row in series]
    variances = [row["variance"] if row["variance"] is not None else float("nan") for row in series]
    deltas = [row["delta_pct"] if row["delta_pct"] is not None else float("nan") for row in series]

    stable = find_stability_point(series, threshold_pct=threshold)
    stable_k = stable["file_count"] if stable else None

    fig, axes = plt.subplots(2, 1, figsize=(10, 8), sharex=True)
    fig.suptitle(f"Receiver variance stability — {scenario} / {metric}", fontsize=12)

    axes[0].plot(k, variances, "s-", linewidth=1.5, markersize=4, label="Var(X_k)")
    if stable_k is not None:
        axes[0].axvline(stable_k, color="green", linestyle=":", linewidth=1.5, label=f"Stable at k={stable_k}")
    axes[0].axvline(32, color="gray", linestyle="--", linewidth=1, alpha=0.7, label="k=32 reference")
    axes[0].set_ylabel("Variance")
    axes[0].grid(True, alpha=0.25)
    axes[0].legend(fontsize=8)

    axes[1].plot(k, deltas, "D-", linewidth=1.5, markersize=4, label="|ΔVar|/Var_{k-1} (%)")
    axes[1].axhline(threshold, color="red", linestyle="--", linewidth=1, label=f"Threshold {threshold}%")
    if stable_k is not None:
        axes[1].axvline(stable_k, color="green", linestyle=":", linewidth=1.5)
    axes[1].axvline(32, color="gray", linestyle="--", linewidth=1, alpha=0.7)
    axes[1].set_xlabel("Sample size k (files)")
    axes[1].set_ylabel("Relative Δvariance (%)")
    axes[1].grid(True, alpha=0.25)
    axes[1].legend(fontsize=8)

    safe_scenario = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(scenario))
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(out_dir, f"receiver_variance_stability_{safe_scenario}_{metric}_{ts}.png")

    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"Plot saved: {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Find receiver checkpoint stability using variance change: "
            "Var(X_k) ≈ Var(X_(k+n))."
        )
    )
    parser.add_argument("--receiver-db", default=config.DB_PATH, help="Path to receiver SQLite DB.")
    parser.add_argument("--scenario", default=None, help="Scenario filter (e.g., los, nlos).")
    parser.add_argument(
        "--metric",
        default="all",
        choices=["all"] + METRIC_CHOICES,
        help="Metric to analyse. Use 'all' to process all receiver metrics.",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        help="Max relative change in variance (%) to declare stability (default: 5.0).",
    )
    parser.add_argument(
        "--out-dir",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "modeling_reports", "statistical_reports"),
        help="Output directory for plots.",
    )
    args = parser.parse_args()

    if not os.path.exists(args.receiver_db):
        sys.exit(f"Receiver DB not found: {args.receiver_db}")

    metrics = METRIC_CHOICES if args.metric == "all" else [args.metric]
    all_rows = fetch_receiver_checkpoint_history(db_path=args.receiver_db, scenario=args.scenario)
    if not all_rows:
        sys.exit(
            "No checkpoint history found. "
            "Run generate_statistical_report.py at least once per transfer batch first."
        )

    scenarios = sorted({str(row.get("scenario") or "unknown") for row in all_rows})
    generated = 0

    for scenario in scenarios:
        for metric in metrics:
            metric_rows = [
                row for row in all_rows
                if str(row.get("scenario") or "unknown") == scenario and str(row.get("metric")) == metric
            ]
            if not metric_rows:
                continue

            series = variance_stability_series(metric_rows)
            print_table(scenario, metric, series, args.threshold)
            plot_series(scenario, metric, series, args.threshold, args.out_dir)
            generated += 1

    print(f"\n{generated} variance stability plot(s) generated.")


if __name__ == "__main__":
    main()
