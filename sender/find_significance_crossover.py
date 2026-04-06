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
from db_utils import fetch_checkpoint_history


def variance_stability_series(rows):

    result = []
    prev_variance = None
    for row in rows:
        variance = row.get("variance_value")
        file_count = row["file_count"]
        if variance is None:
            result.append({
                "file_count": file_count,
                "variance": None,
                "delta_pct": None,
                "mean": row.get("mean_value"),
                "std": row.get("std_value"),
                "n_reports": row.get("n_reports", 1),
            })
            prev_variance = None
            continue

        if prev_variance is None or prev_variance == 0:
            delta_pct = None
        else:
            delta_pct = abs(variance - prev_variance) / prev_variance * 100.0

        result.append({
            "file_count": file_count,
            "variance": variance,
            "delta_pct": delta_pct,
            "mean": row.get("mean_value"),
            "std": row.get("std_value"),
            "n_reports": row.get("n_reports", 1),
        })
        prev_variance = variance

    return result


def find_stability_point(stability_series, threshold_pct=5.0):
    for i, row in enumerate(stability_series):
        delta = row["delta_pct"]
        if delta is None:
            continue
        if delta <= threshold_pct:
            if i + 1 < len(stability_series):
                next_delta = stability_series[i + 1]["delta_pct"]
                if next_delta is not None and next_delta <= threshold_pct:
                    return row["file_count"], delta
            else:
                return row["file_count"], delta
    return None, None


def find_nearest_stability(stability_series):
    """Return the checkpoint with the smallest variance delta."""
    best = None
    best_delta = float("inf")
    for row in stability_series:
        if row["delta_pct"] is not None and row["delta_pct"] < best_delta:
            best_delta = row["delta_pct"]
            best = row
    return best, best_delta


def main():
    parser = argparse.ArgumentParser(
        description=(
            "Find the minimum sample size k at which Var(X_k) stabilizes.\n"
            "Implements: Var(X_k) ≈ Var(X_{k+n}) — no need to go beyond k."
        )
    )
    parser.add_argument("--sender-db", default=config.DB_PATH)
    parser.add_argument(
        "--metric",
        default="send_span_s",
        help="Metric to analyse (default: send_span_s).",
    )
    parser.add_argument(
        "--scenario-name",
        default="overall",
        help="Scenario label used for report file naming (default: overall).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=5.0,
        help=(
            "Max relative change in variance between consecutive checkpoints "
            "to declare stability, in percent (default: 5.0)."
        ),
    )
    parser.add_argument(
        "--out-dir",
        default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "modeling_reports",
            "statistical_reports",
        ),
    )
    args = parser.parse_args()

    if not os.path.exists(args.sender_db):
        sys.exit(f"Sender DB not found: {args.sender_db}")

    rows = fetch_checkpoint_history(args.sender_db, metric_column=args.metric)
    if not rows:
        sys.exit(
            "No checkpoint history found. "
            "Run generate_statistical_report.py at least once first."
        )

    output_lines = []

    def emit(line: str = ""):
        print(line)
        output_lines.append(line)

    stability = variance_stability_series(rows)

    # Print table
    emit(f"\nVariance stability analysis — metric: {args.metric}")
    emit(f"Criterion: |Var(X_k) - Var(X_{{k-1}})| / Var(X_{{k-1}}) < {args.threshold}%")
    header = (
        f"{'k':>6}  {'mean (s)':>10}  {'variance (s²)':>14}  "
        f"{'std (s)':>10}  {'Δvar %':>10}  {'n_reports':>9}"
    )
    emit(header)
    emit("-" * len(header))

    for row in stability:
        delta_str = f"{row['delta_pct']:10.2f}" if row["delta_pct"] is not None else f"{'—':>10}"
        var_str = f"{row['variance']:14.4f}" if row["variance"] is not None else f"{'NA':>14}"
        mean_str = f"{row['mean']:10.4f}" if row["mean"] is not None else f"{'NA':>10}"
        std_str = f"{row['std']:10.4f}" if row["std"] is not None else f"{'NA':>10}"
        emit(
            f"{row['file_count']:>6}  {mean_str}  {var_str}  "
            f"{std_str}  {delta_str}  {row['n_reports']:>9}"
        )

    # Find and report stability point
    stable_k, stable_delta = find_stability_point(stability, threshold_pct=args.threshold)
    emit()
    if stable_k is not None:
        emit(
            f"Variance stabilizes at k = {stable_k} "
            f"(Δvar = {stable_delta:.2f}% < {args.threshold}%)"
        )
        if stable_k <= 32:
            emit(
                f"This is within the expected range (≤ 32). "
                f"No need to collect more than {stable_k} samples."
            )
        else:
            emit(
                f"This exceeds 32 samples. Consider whether the scenario "
                f"has higher natural variance than expected."
            )
    else:
        nearest, nearest_delta = find_nearest_stability(stability)
        nearest_text = ""
        if nearest:
            nearest_text = (
                f" Closest point: k={nearest['file_count']} "
                f"with Δvar={nearest_delta:.2f}%."
            )
        emit(
            f"Variance has not stabilized within {args.threshold}% threshold "
            f"across available data.{nearest_text}"
        )
        emit("Consider collecting more samples or raising --threshold.")

    # --- Plot ---
    os.makedirs(args.out_dir, exist_ok=True)
    file_counts = [r["file_count"] for r in stability]
    variances = [r["variance"] for r in stability]
    deltas = [r["delta_pct"] for r in stability]
    means = [r["mean"] for r in stability]
    stds = [r["std"] for r in stability]

    fig, axes = plt.subplots(3, 1, figsize=(10, 11), sharex=True)
    fig.suptitle(
        f"Variance stability analysis — {args.metric}\n"
        f"Criterion: Var(X_k) ≈ Var(X_{{k+n}}) within {args.threshold}%",
        fontsize=12,
    )

    # Panel 1: variance over k
    ax1 = axes[0]
    var_clean = [v if v is not None else float("nan") for v in variances]
    ax1.plot(file_counts, var_clean, "s-", color="#FF5722",
             linewidth=1.5, markersize=4, label="Var(X_k)")
    if stable_k:
        ax1.axvline(stable_k, color="green", linestyle=":",
                    linewidth=1.5, label=f"Stable at k={stable_k}")
    ax1.axvline(32, color="gray", linestyle="--",
                linewidth=1, alpha=0.6, label="k=32 reference")
    ax1.set_ylabel("Variance (s²)")
    ax1.set_title("Sample variance Var(X_k) vs sample size k")
    ax1.legend(fontsize=8)
    ax1.grid(True, alpha=0.25)

    # Panel 2: relative change in variance
    ax2 = axes[1]
    delta_clean = [d if d is not None else float("nan") for d in deltas]
    ax2.plot(file_counts, delta_clean, "D-", color="#9C27B0",
             linewidth=1.5, markersize=4, label="|ΔVar| / Var_{k-1} (%)")
    ax2.axhline(args.threshold, color="red", linestyle="--",
                linewidth=1, label=f"Threshold {args.threshold}%")
    if stable_k:
        ax2.axvline(stable_k, color="green", linestyle=":",
                    linewidth=1.5)
    ax2.axvline(32, color="gray", linestyle="--",
                linewidth=1, alpha=0.6)
    ax2.set_ylabel("Relative Δvariance (%)")
    ax2.set_title("Relative change in variance between consecutive checkpoints")
    ax2.legend(fontsize=8)
    ax2.grid(True, alpha=0.25)

    # Panel 3: mean with CI band
    ax3 = axes[2]
    mean_clean = np.array([m if m is not None else float("nan") for m in means])
    std_clean = np.array([s if s is not None else float("nan") for s in stds])
    ax3.plot(file_counts, mean_clean, "o-", color="#2196F3",
             linewidth=1.5, markersize=4, label="Mean(X_k)")
    ax3.fill_between(
        file_counts,
        mean_clean - std_clean,
        mean_clean + std_clean,
        alpha=0.15, color="#2196F3", label="Mean ± 1 std",
    )
    if stable_k:
        ax3.axvline(stable_k, color="green", linestyle=":",
                    linewidth=1.5, label=f"Stable at k={stable_k}")
    ax3.axvline(32, color="gray", linestyle="--",
                linewidth=1, alpha=0.6)
    ax3.set_xlabel("Sample size k (number of files)")
    ax3.set_ylabel(f"{args.metric} (s)")
    ax3.set_title("Mean and spread — shown for context only")
    ax3.legend(fontsize=8)
    ax3.grid(True, alpha=0.25)

    safe_scenario = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in str(args.scenario_name))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    plt.tight_layout()
    out_path = os.path.join(
        args.out_dir,
        f"variance_stability_{safe_scenario}_{args.metric}_{timestamp}.png",
    )
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    emit(f"\nPlot saved: {out_path}")

    txt_report = os.path.join(
        args.out_dir,
        f"variance_stability_{safe_scenario}_{args.metric}_{timestamp}.txt",
    )
    with open(txt_report, "w") as handle:
        handle.write("\n".join(output_lines) + "\n")
    print(f"Text report saved: {txt_report}")


if __name__ == "__main__":
    main()