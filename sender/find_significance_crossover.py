#!/usr/bin/env python3
"""
Reads accumulated checkpoint statistics from the sender DB and finds
where mean and standard deviation converge for a chosen metric.
"""
import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

import config
from db_utils import fetch_checkpoint_history


def find_crossover(rows, tolerance_pct=10.0):
    """
    Return the first row where abs(mean - std) / max(mean, std)
    is within tolerance_pct.
    """
    for row in rows:
        mean = row["mean_value"]
        std = row["std_value"]
        if mean is None or std is None:
            continue
        denom = max(abs(mean), abs(std), 1e-9)
        rel_gap_pct = abs(mean - std) / denom * 100.0
        if rel_gap_pct <= tolerance_pct:
            return row, rel_gap_pct
    return None, None


def find_nearest(rows):
    """Return the checkpoint row with the smallest relative mean-std gap."""
    best = None
    for row in rows:
        mean = row["mean_value"]
        std = row["std_value"]
        if mean is None or std is None:
            continue
        denom = max(abs(mean), abs(std), 1e-9)
        rel_gap_pct = abs(mean - std) / denom * 100.0
        if best is None or rel_gap_pct < best[1]:
            best = (row, rel_gap_pct)
    return best


def main():
    parser = argparse.ArgumentParser(
        description="Find statistical crossover from accumulated checkpoint history."
    )
    parser.add_argument("--sender-db", default=config.DB_PATH)
    parser.add_argument(
        "--metric",
        default="send_span_s",
        help="Metric column to analyse (default: send_span_s).",
    )
    parser.add_argument(
        "tolerance",
        nargs="?",
        type=float,
        default=10.0,
        help="Relative gap tolerance in percent for mean≈std (default: 10.0).",
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
            "No checkpoint history found in DB. Run generate_statistical_report.py first."
        )

    print(f"\nAccumulated checkpoint history for metric: {args.metric}")
    print(f"{'files':>6}  {'mean_s':>10}  {'variance_s2':>12}  {'std_s':>10}  {'rel_gap_mean_std_%':>19}  {'n_reports':>9}")
    print("-" * 62)

    file_counts = []
    means = []
    variances = []
    stds = []
    rel_gaps = []

    for row in rows:
        mean = row["mean_value"]
        variance = row["variance_value"]
        std = row["std_value"]
        denom = max(abs(mean), abs(std if std is not None else 0.0), 1e-9)
        rel_gap = abs(mean - (std if std is not None else 0.0)) / denom * 100.0

        file_counts.append(row["file_count"])
        means.append(mean)
        variances.append(variance)
        stds.append(std if std is not None else 0.0)
        rel_gaps.append(rel_gap)

        print(
            f"{row['file_count']:>6}  "
            f"{mean:>10.4f}  "
            f"{variance:>12.4f}  "
            f"{(std if std is not None else 0.0):>10.4f}  "
            f"{rel_gap:>19.2f}  "
            f"{row['n_reports']:>9}"
        )

    crossover_row, crossover_gap = find_crossover(rows, tolerance_pct=args.tolerance)
    if crossover_row:
        print(
            f"\nCrossover found at file_count = {crossover_row['file_count']} "
            f"(mean-std rel_gap = {crossover_gap:.2f}% <= {args.tolerance}%)"
        )
    else:
        nearest = find_nearest(rows)
        nearest_text = ""
        if nearest is not None:
            nearest_row, nearest_gap = nearest
            nearest_text = (
                f" Nearest checkpoint is file_count={nearest_row['file_count']} "
                f"with mean-std rel_gap={nearest_gap:.2f}%."
            )
        print(
            f"\nNo crossover found within tolerance {args.tolerance}%. "
            f"More runs may be needed.{nearest_text}"
        )

    os.makedirs(args.out_dir, exist_ok=True)
    fig, axes = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

    ax1 = axes[0]
    ax1.plot(file_counts, means, "o-", label="Mean", color="#2196F3")
    ax1.plot(file_counts, variances, "s--", label="Variance", color="#FF5722")
    if stds:
        means_arr = np.array(means)
        stds_arr = np.array(stds)
        ax1.fill_between(
            file_counts,
            means_arr - stds_arr,
            means_arr + stds_arr,
            alpha=0.15,
            color="#2196F3",
            label="Mean ± 1 std",
        )
    if crossover_row:
        ax1.axvline(
            x=crossover_row["file_count"],
            color="green",
            linestyle=":",
            linewidth=1.5,
            label=f"Crossover at n={crossover_row['file_count']}",
        )
    ax1.set_ylabel(f"{args.metric} (s)")
    ax1.set_title(f"Mean / Std trend — {args.metric}")
    ax1.legend()
    ax1.grid(True, alpha=0.3)

    ax2 = axes[1]
    ax2.plot(file_counts, rel_gaps, "D-", color="#9C27B0", label="Relative gap (mean vs std) %")
    ax2.axhline(
        y=args.tolerance,
        color="red",
        linestyle="--",
        linewidth=1,
        label=f"Tolerance = {args.tolerance}%",
    )
    if crossover_row:
        ax2.axvline(
            x=crossover_row["file_count"],
            color="green",
            linestyle=":",
            linewidth=1.5,
        )
    ax2.set_xlabel("Number of files (cumulative)")
    ax2.set_ylabel("Relative gap |mean - std| / max (%)")
    ax2.set_title("Convergence gap — lower is more stable")
    ax2.legend()
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    out_path = os.path.join(args.out_dir, f"significance_crossover_{args.metric}.png")
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f"\nPlot saved to: {out_path}")


if __name__ == "__main__":
    main()
