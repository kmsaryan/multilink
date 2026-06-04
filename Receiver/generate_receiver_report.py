#!/usr/bin/env python3
import argparse
import os
import sqlite3
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

import config

DEFAULT_DB  = config.DB_PATH
DEFAULT_OUT = os.path.join(config.RESULTS_DIR, "statistical_reports")

SCENARIO_ORDER = [
    ("no_shaper",     "No-Shaper", "#4C72B0"),
    ("los",           "LOS",       "#55A868"),
    ("nlos",          "NLOS",      "#C44E52"),
    ("los_linkfail",  "LOS-LF",    "#8172B2"),
    ("nlos_linkfail", "NLOS-LF",   "#CCB974"),
]


def load_data(db_path):
    if not os.path.exists(db_path):
        sys.exit(f"Database not found: {db_path}")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        "SELECT scenario, file_to_file_time_s FROM run_statistics "
        "WHERE file_to_file_time_s IS NOT NULL AND file_to_file_time_s > 0",
        conn,
    )
    conn.close()
    if df.empty:
        sys.exit("No valid file_to_file_time_s rows found.")
    df["scenario"] = df["scenario"].str.strip().str.lower()
    return df


def plot(df, out_dir, y_percentile=95.0, y_headroom=0.10):
    os.makedirs(out_dir, exist_ok=True)
    present = set(df["scenario"].unique())
    ordered = [(k, l, c) for k, l, c in SCENARIO_ORDER if k in present]
    if not ordered:
        sys.exit(f"No matching scenarios found. DB has: {sorted(present)}")

    keys    = [s[0] for s in ordered]
    labels  = [s[1] for s in ordered]
    colours = [s[2] for s in ordered]
    groups  = [df.loc[df["scenario"] == k, "file_to_file_time_s"].values for k in keys]

    # y-axis cap: configurable percentile across all data + headroom
    all_vals = np.concatenate(groups)
    y_cap    = np.percentile(all_vals, y_percentile) * (1.0 + y_headroom)

    fig, ax = plt.subplots(figsize=(13, 7))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    x_pos = np.arange(1, len(ordered) + 1)
    rng   = np.random.default_rng(42)

    for i, (vals, colour, label) in enumerate(zip(groups, colours, labels)):
        x = x_pos[i]

        # split into in-range and outlier sets
        in_range = vals[vals <= y_cap]
        outliers = vals[vals >  y_cap]

        ax.boxplot(
            vals, positions=[x], widths=0.52, patch_artist=True,
            showfliers=False,
            whiskerprops=dict(color="#333333", linewidth=1.5, linestyle="--"),
            capprops=dict(color="#333333", linewidth=1.8),
            medianprops=dict(color="#111111", linewidth=2.5),
            boxprops=dict(facecolor=colour + "55", edgecolor=colour, linewidth=2.0),
        )

        # jitter — in-range only
        jitter = rng.uniform(-0.14, 0.14, size=len(in_range))
        ax.scatter(x + jitter, in_range,
                   color=colour, alpha=0.70, s=30,
                   zorder=3, edgecolors="white", linewidths=0.5)

        # outlier markers drawn at y_cap with a triangle + count label
        if len(outliers) > 0:
            ax.scatter([x] * len(outliers), [y_cap * 0.985] * len(outliers),
                       marker="^", color=colour, s=80, zorder=5,
                       edgecolors="white", linewidths=0.8)
            ax.text(x + 0.18, y_cap * 0.985,
                    f"+{len(outliers)} outlier{'s' if len(outliers) > 1 else ''}",
                    va="center", fontsize=7.5, color=colour, style="italic")

        # mean diamond
        mean_val = float(np.mean(vals))
        ax.plot(x, min(mean_val, y_cap * 0.96),
                marker="D", color="white",
                markeredgecolor=colour, markeredgewidth=2.4,
                markersize=11, zorder=6)

        # mean label — placed to the right to avoid overlapping the box
        ax.text(x + 0.28, min(mean_val, y_cap * 0.96),
                f"{mean_val:.1f}s",
                va="center", ha="left", fontsize=8.5,
                color=colour, fontweight="bold", zorder=7)

        # n= label below x-tick
        ax.text(x, -y_cap * 0.045, f"n={len(vals)}",
                ha="center", va="top", fontsize=9, color="#666666")

    ax.set_xticks(x_pos)
    ax.set_xticklabels(labels, fontsize=13)
    ax.set_ylabel("Receiver E2E Delivery Time (s)", fontsize=12, labelpad=10)
    ax.set_xlabel("Scenario", fontsize=12, labelpad=28)
    ax.set_title(
        "Receiver End-to-End Delivery Time per Scenario\n"
        r"$\mathit{file\_to\_file\_time\_s}$  ·  n = 40 per scenario",
        fontsize=13, fontweight="bold", pad=16, linespacing=1.6,
    )
    ax.set_xlim(0.35, len(ordered) + 0.65)
    ax.set_ylim(-y_cap * 0.06, y_cap)

    # cap line annotation
    ax.axhline(y_cap, color="#AAAAAA", linewidth=0.8, linestyle=":")
    ax.text(len(ordered) + 0.55, y_cap,
            f"cap = {y_cap:.0f}s\n({y_percentile:g}th pct)", va="center",
            fontsize=7.5, color="#999999", ha="right")

    median_patch = mpatches.Patch(facecolor="none", edgecolor="#111111",
                                   linewidth=2.5, label="Median")
    mean_marker  = plt.Line2D([0], [0], marker="D", color="w",
                               markeredgecolor="#555555", markeredgewidth=2.0,
                               markersize=10, label="Mean", linestyle="None")
    ax.legend(handles=[median_patch, mean_marker], loc="upper right",
              fontsize=10, framealpha=0.9, edgecolor="#CCCCCC")

    ax.yaxis.grid(True, linestyle="--", alpha=0.4, color="#CCCCCC", zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(axis="both", labelsize=11)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "receiver_e2e_boxplot.png")
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close()
    print(f"Saved -> {out_path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",  default=DEFAULT_DB)
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument(
        "--y-percentile",
        type=float,
        default=95.0,
        help="Upper y-axis percentile used to cap the plot (default: 95)",
    )
    parser.add_argument(
        "--y-headroom",
        type=float,
        default=0.10,
        help="Extra headroom above the percentile cap as a fraction (default: 0.10)",
    )
    args = parser.parse_args()
    df = load_data(args.db)
    print(f"Loaded {len(df)} rows | scenarios: {sorted(df['scenario'].unique())}")
    plot(df, args.out, y_percentile=args.y_percentile, y_headroom=args.y_headroom)


if __name__ == "__main__":
    main()