#!/usr/bin/env python3
import argparse
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np

try:
    import config
except Exception:
    config = None

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_REPORT_DIR = os.path.join(_BASE_DIR, "modeling_reports", "statistical_reports")
_DEFAULT_DB = getattr(config, "DB_PATH", "sender_state.db")
_DEFAULT_OUT = getattr(config, "RESULTS_DIR", _DEFAULT_REPORT_DIR)

# ── scenario display order ────────────────────────────────────────────────
SCENARIO_ORDER = ["No-Shaper", "LOS", "NLOS", "LOS-LF", "NLOS-LF"]

_PALETTE = {
    "No-Shaper": "#4e9bbf",
    "LOS":       "#5ab26e",
    "NLOS":      "#e07b52",
    "LOS-LF":    "#9b7ec8",
    "NLOS-LF":   "#d4575a",
}


# ── data loading ──────────────────────────────────────────────────────────

def load_data(db_path: str) -> dict[str, list[float]]:
    """Return {scenario: [send_span_s, ...]} from run_statistics."""
    import sqlite3
    if not os.path.exists(db_path):
        sys.exit(f"[ERROR] Sender DB not found: {db_path}")

    conn = sqlite3.connect(db_path)
    try:
        cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(run_statistics)").fetchall()
        }
        if "send_span_s" in cols:
            value_expr = "send_span_s"
            where_clause = "send_span_s IS NOT NULL"
        elif {"first_last_sent", "last_last_sent"}.issubset(cols):
            value_expr = "(last_last_sent - first_last_sent)"
            where_clause = "first_last_sent IS NOT NULL AND last_last_sent IS NOT NULL"
        else:
            sys.exit(
                "[ERROR] run_statistics has no usable span columns "
                "(expected send_span_s or first_last_sent/last_last_sent)."
            )

        rows = conn.execute(
            f"""
            SELECT scenario, {value_expr} AS send_span_s
            FROM   run_statistics
            WHERE  {where_clause}
            """
        ).fetchall()
    except sqlite3.OperationalError as exc:
        sys.exit(f"[ERROR] Query failed: {exc}")
    finally:
        conn.close()

    if not rows:
        sys.exit("[ERROR] No send_span_s rows found in run_statistics.")

    groups: dict[str, list[float]] = {}
    for scenario, value in rows:
        groups.setdefault(str(scenario or "unknown"), []).append(float(value))
    return groups


# ── plot ──────────────────────────────────────────────────────────────────

def plot(data: dict[str, list[float]], out_path: str) -> None:
    order  = [s for s in SCENARIO_ORDER if s in data]
    order += sorted(s for s in data if s not in SCENARIO_ORDER)

    box_data  = [data[sc] for sc in order]
    positions = list(range(1, len(order) + 1))

    fig, ax = plt.subplots(figsize=(max(8, len(order) * 1.8), 6))

    # ── boxes ─────────────────────────────────────────────────────────────
    bp = ax.boxplot(
        box_data,
        positions=positions,
        widths=0.45,
        patch_artist=True,
        showfliers=False,
        medianprops=dict(color="black", linewidth=2.2),
        whiskerprops=dict(color="#555", linewidth=1.2, linestyle="--"),
        capprops=dict(color="#555", linewidth=1.4),
        boxprops=dict(linewidth=1.2),
    )
    for patch, label in zip(bp["boxes"], order):
        patch.set_facecolor(_PALETTE.get(label, "#aaaaaa"))
        patch.set_alpha(0.35)

    # ── jitter strip ──────────────────────────────────────────────────────
    rng = np.random.default_rng(42)
    for pos, label in zip(positions, order):
        vals = np.array(data[label])
        jx   = pos + rng.uniform(-0.18, 0.18, size=len(vals))
        ax.scatter(jx, vals,
                   color=_PALETTE.get(label, "#aaaaaa"),
                   s=14, alpha=0.65, linewidths=0, zorder=3)

    # ── mean line + diamond ───────────────────────────────────────────────
    mean_annots = []
    for pos, label in zip(positions, order):
        vals  = np.array(data[label])
        mean  = float(np.mean(vals))
        color = _PALETTE.get(label, "#aaaaaa")

        ax.plot([pos - 0.225, pos + 0.225], [mean, mean],
                color=color, linewidth=2.2, zorder=5,
                solid_capstyle="butt")
        ax.scatter([pos], [mean],
                   marker="D", s=50,
                   facecolor="white", edgecolors=color,
                   linewidths=1.8, zorder=6)
        mean_annots.append((pos, mean, color))

    # annotate after axes are settled
    for pos, mean, color in mean_annots:
        ax.annotate(f"{mean:.1f}s",
                    xy=(pos, mean), xytext=(5, 6),
                    textcoords="offset points",
                    fontsize=7.5, color=color, fontweight="bold", zorder=7)

    # ── formatting ────────────────────────────────────────────────────────
    ax.set_xticks(positions)
    ax.set_xticklabels(
        [f"{sc}\n(n={len(data[sc])})" for sc in order],
        fontsize=10,
    )
    ax.set_ylabel("Sender T_span (s)", fontsize=11)
    ax.set_title("Sender Active Dispatch Span (T_span) per Scenario",
                 fontsize=12, fontweight="bold", pad=14)
    ax.grid(axis="y", linestyle="--", alpha=0.45, zorder=0)
    ax.set_xlim(0.3, len(order) + 0.7)

    legend_elements = [
        mpatches.Patch(facecolor="#cccccc", edgecolor="black", label="IQR (box)"),
        plt.Line2D([0], [0], color="black", linewidth=2, label="Median"),
        plt.Line2D([0], [0], color="#666", linewidth=2,
                   marker="D", markerfacecolor="white",
                   markeredgecolor="#666", markersize=7,
                   label="Mean"),
        plt.Line2D([0], [0], marker="o", color="w",
                   markerfacecolor="#888", markersize=6,
                   label="Individual payload"),
    ]
    ax.legend(handles=legend_elements, fontsize=8.5,
              loc="upper left", framealpha=0.85)

    fig.tight_layout()
    fig.savefig(out_path, dpi=300)
    plt.close(fig)
    print(f"[OK] Plot saved → {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────

def main() -> None:
    p = argparse.ArgumentParser(
        description="Box plot of send_span_s per scenario from run_statistics."
    )
    p.add_argument("--db",  default=_DEFAULT_DB,
                   help=f"Sender SQLite DB (default: {_DEFAULT_DB})")
    p.add_argument("--out", default=_DEFAULT_OUT,
                   help=f"Output directory (default: {_DEFAULT_OUT})")
    args = p.parse_args()

    os.makedirs(args.out, exist_ok=True)

    print(f"[INFO] Reading: {args.db}")
    data = load_data(args.db)

    for sc in SCENARIO_ORDER:
        if sc in data:
            vals = data[sc]
            print(f"  {sc:12s}  n={len(vals):3d}  mean={sum(vals)/len(vals):.3f}s")

    plot(data, os.path.join(args.out, "sender_tspan_boxplot.png"))


if __name__ == "__main__":
    main()