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

SCENARIO_ORDER = ["No-Shaper", "LOS", "NLOS", "LOS-LF", "NLOS-LF"]

_PALETTE = {
    "No-Shaper": "#1f77b4",
    "LOS":       "#2ca02c",
    "NLOS":      "#ff7f0e",
    "LOS-LF":    "#9467bd",
    "NLOS-LF":   "#d62728",
}

# normalise any variant coming out of the DB into the canonical display label
def canonical_scenario_label(label: str) -> str:
    normalized = str(label or "unknown").strip().lower().replace("_", "-")
    aliases = {
        "no-shaper":         "No-Shaper",
        "noshaper":          "No-Shaper",
        "unknown":           "No-Shaper",
        "los":               "LOS",
        "nlos":              "NLOS",
        "los-lf":            "LOS-LF",
        "nlos-lf":           "NLOS-LF",
        "los-link-failure":  "LOS-LF",
        "nlos-link-failure": "NLOS-LF",
        "los-linkfail":      "LOS-LF",
        "nlos-linkfail":     "NLOS-LF",
        "los-linkfailure":   "LOS-LF",
        "nlos-linkfailure":  "NLOS-LF",
    }
    return aliases.get(normalized, str(label or "unknown"))


def load_data(db_path):
    if not os.path.exists(db_path):
        sys.exit(f"[ERROR] Database not found: {db_path}")
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(
        "SELECT scenario, file_to_file_time_s FROM run_statistics "
        "WHERE file_to_file_time_s IS NOT NULL AND file_to_file_time_s > 0",
        conn,
    )
    conn.close()
    if df.empty:
        sys.exit("[ERROR] No valid file_to_file_time_s rows found.")
    df["scenario"] = df["scenario"].apply(canonical_scenario_label)
    return df


def plot(df, out_dir, y_percentile=95.0, y_headroom=0.10):
    os.makedirs(out_dir, exist_ok=True)
    present = set(df["scenario"].unique())
    order   = [s for s in SCENARIO_ORDER if s in present]
    order  += sorted(s for s in present if s not in SCENARIO_ORDER)
    if not order:
        sys.exit(f"[ERROR] No matching scenarios found. DB has: {sorted(present)}")

    groups = [df.loc[df["scenario"] == sc, "file_to_file_time_s"].values for sc in order]
    counts = [len(v) for v in groups]

    # y-axis cap: configurable percentile + headroom
    all_vals = np.concatenate(groups)
    y_cap    = np.percentile(all_vals, y_percentile) * (1.0 + y_headroom)

    fig, ax = plt.subplots(figsize=(max(8, len(order) * 1.8), 6))
    fig.patch.set_facecolor("white")
    ax.set_facecolor("white")

    positions = list(range(1, len(order) + 1))

    # draw all boxes in one call, then re-colour edges per scenario
    bp = ax.boxplot(
        groups,
        positions=positions,
        widths=0.45,
        patch_artist=True,
        showfliers=False,
        medianprops=dict(color="#222222", linewidth=2.2),
        whiskerprops=dict(color="#555555", linewidth=1.2, linestyle="--"),
        capprops=dict(color="#555555", linewidth=1.4),
        boxprops=dict(linewidth=1.6),
    )
    for patch, sc in zip(bp["boxes"], order):
        colour = _PALETTE.get(sc, "#aaaaaa")
        patch.set_facecolor(colour)
        patch.set_alpha(0.5)
        patch.set_edgecolor(colour)

    # mean diamond + annotated value (above); median annotated value (below)
    mean_annots   = []
    median_annots = []
    for pos, sc in zip(positions, order):
        vals   = np.array(groups[order.index(sc)])
        colour = _PALETTE.get(sc, "#aaaaaa")
        mean   = float(np.mean(vals))
        median = float(np.median(vals))
        y_mean = min(mean, y_cap * 0.96)

        # coloured mean line across box width
        ax.plot([pos - 0.225, pos + 0.225], [y_mean, y_mean],
                color=colour, linewidth=2.2, zorder=5, solid_capstyle="butt")
        # white diamond with coloured edge
        ax.scatter([pos], [y_mean],
                   marker="D", s=50,
                   facecolor="white", edgecolors=colour,
                   linewidths=1.8, zorder=6)
        mean_annots.append((pos, y_mean, colour, mean))
        median_annots.append((pos, min(median, y_cap * 0.96)))

    # annotate mean above diamond
    for pos, y, colour, raw in mean_annots:
        ax.annotate(f"{raw:.1f}s",
                    xy=(pos, y), xytext=(5, 6),
                    textcoords="offset points",
                    fontsize=7.5, color=colour, fontweight="bold", zorder=7)

    # annotate median below its line
    for pos, y in median_annots:
        ax.annotate(f"{float(np.median(groups[pos - 1])):.1f}s",
                    xy=(pos, y), xytext=(5, -14),
                    textcoords="offset points",
                    fontsize=7.5, color="#222222", fontweight="bold", zorder=7)

    ax.set_xticks(positions)
    ax.set_xticklabels(
        [f"{sc}\n(n={cnt})" for sc, cnt in zip(order, counts)],
        fontsize=10,
    )
    ax.set_ylabel("Receiver E2E Delivery Time (s)", fontsize=11, labelpad=10)
    ax.set_xlabel("Scenario", fontsize=11, labelpad=12)
    ax.set_title(
        "Receiver End-to-End Delivery Time per Scenario\n"
        r"$\mathit{file\_to\_file\_time\_s}$  ·  n = 40 per scenario",
        fontsize=12, fontweight="bold", pad=14, linespacing=1.6,
    )
    ax.set_xlim(0.3, len(order) + 0.7)
    ax.set_ylim(0, y_cap)
    ax.grid(axis="y", linestyle="--", alpha=0.45, zorder=0)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.tick_params(axis="both", labelsize=11)

    # cap line annotation
    ax.axhline(y_cap, color="#AAAAAA", linewidth=0.8, linestyle=":")
    ax.text(len(order) + 0.55, y_cap,
            f"cap = {y_cap:.0f}s\n({y_percentile:g}th pct)",
            va="center", fontsize=7.5, color="#999999", ha="right")

    legend_elements = [
        mpatches.Patch(facecolor="#cccccc", edgecolor="black", label="IQR (box)"),
        plt.Line2D([0], [0], color="#222222", linewidth=2, label="Median"),
        plt.Line2D([0], [0], color="#666666", linewidth=2,
                   marker="D", markerfacecolor="white",
                   markeredgecolor="#666666", markersize=7,
                   label="Mean"),
    ]
    ax.legend(handles=legend_elements, fontsize=8.5,
              loc="upper left", framealpha=0.85)

    fig.tight_layout()
    out_path = os.path.join(out_dir, "receiver_e2e_boxplot.png")
    fig.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)
    print(f"[OK] Plot saved → {out_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Box plot of file_to_file_time_s per scenario from run_statistics."
    )
    parser.add_argument("--db",  default=DEFAULT_DB,
                        help=f"Receiver SQLite DB (default: {DEFAULT_DB})")
    parser.add_argument("--out", default=DEFAULT_OUT,
                        help=f"Output directory (default: {DEFAULT_OUT})")
    parser.add_argument("--y-percentile", type=float, default=95.0,
                        help="Upper y-axis percentile cap (default: 95)")
    parser.add_argument("--y-headroom",   type=float, default=0.10,
                        help="Headroom fraction above percentile cap (default: 0.10)")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)
    print(f"[INFO] Reading: {args.db}")
    df = load_data(args.db)

    for sc in SCENARIO_ORDER:
        sub = df.loc[df["scenario"] == sc, "file_to_file_time_s"]
        if not sub.empty:
            print(f"  {sc:12s}  n={len(sub):3d}  mean={sub.mean():.3f}s")

    plot(df, args.out, y_percentile=args.y_percentile, y_headroom=args.y_headroom)


if __name__ == "__main__":
    main()