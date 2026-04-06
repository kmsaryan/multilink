#!/usr/bin/env python3
"""
Reads accumulated scenario_statistics from the receiver DB across all
report runs and finds the sample count at which mean ≈ variance for
chunk_to_chunk_time, file_to_file_time, and goodput simultaneously.
Produces a multi-panel convergence plot per scenario.
"""
import argparse
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np

import config
from db_utils import fetch_scenario_statistics_history


METRICS = [
    ("ctc_mean",     "ctc_variance",  "ctc_ci95",  "Chunk-to-chunk time (s)"),
    ("ftf_mean",     "ftf_variance",  "ftf_ci95",  "File-to-file time (s)"),
    ("gp_mean",      "gp_variance",   "gp_ci95",   "Goodput (Mbps)"),
]


def relative_gap(mean, variance):
    if mean is None or variance is None:
        return None
    denom = max(abs(mean), abs(variance), 1e-9)
    return abs(mean - variance) / denom * 100.0


def find_crossover(rows, mean_key, variance_key, tolerance_pct):
    """
    Return the first row where rel_gap(mean, variance) <= tolerance_pct.
    """
    for row in rows:
        gap = relative_gap(row.get(mean_key), row.get(variance_key))
        if gap is not None and gap <= tolerance_pct:
            return row, gap
    return None, None


def print_scenario_table(scenario, rows, tolerance_pct):
    print(f"\n{'='*70}")
    print(f"Scenario: {scenario}  (tolerance = {tolerance_pct}%)")
    print(f"{'='*70}")

    header = (
        f"{'n':>5}  "
        f"{'ctc_mean':>10} {'ctc_var':>10} {'ctc_gap%':>9}  "
        f"{'ftf_mean':>10} {'ftf_var':>10} {'ftf_gap%':>9}  "
        f"{'gp_mean':>9} {'gp_var':>9} {'gp_gap%':>8}"
    )
    print(header)
    print("-" * len(header))

    for row in rows:
        n = int(row["n_runs"])
        ctc_gap = relative_gap(row.get("ctc_mean"), row.get("ctc_variance"))
        ftf_gap = relative_gap(row.get("ftf_mean"), row.get("ftf_variance"))
        gp_gap  = relative_gap(row.get("gp_mean"),  row.get("gp_variance"))

        def fmt(v):
            return f"{v:10.4f}" if v is not None else f"{'NA':>10}"

        def fmt_gap(v):
            return f"{v:9.2f}" if v is not None else f"{'NA':>9}"

        print(
            f"{n:>5}  "
            f"{fmt(row.get('ctc_mean'))} {fmt(row.get('ctc_variance'))} {fmt_gap(ctc_gap)}  "
            f"{fmt(row.get('ftf_mean'))} {fmt(row.get('ftf_variance'))} {fmt_gap(ftf_gap)}  "
            f"{fmt(row.get('gp_mean')):>9} {fmt(row.get('gp_variance')):>9} {fmt_gap(gp_gap):>8}"
        )

    # Report crossovers
    for mean_key, var_key, _, label in METRICS:
        crossover_row, gap = find_crossover(rows, mean_key, var_key, tolerance_pct)
        if crossover_row:
            print(
                f"\n[{label}] crossover at n={crossover_row['n_runs']} "
                f"(gap={gap:.2f}%)"
            )
        else:
            print(f"\n[{label}] no crossover within {tolerance_pct}% tolerance")


def plot_scenario(scenario, rows, tolerance_pct, out_dir):
    n_values = [row["n_runs"] for row in rows]

    fig = plt.figure(figsize=(14, 11))
    fig.suptitle(
        f"Convergence analysis — scenario: {scenario}\n"
        f"(tolerance = {tolerance_pct}%)",
        fontsize=13,
        y=0.98
    )

    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.45, wspace=0.35)

    for panel_idx, (mean_key, var_key, ci_key, label) in enumerate(METRICS):
        means     = [row.get(mean_key) for row in rows]
        variances = [row.get(var_key)  for row in rows]
        cis       = [row.get(ci_key)   for row in rows]
        gaps      = [relative_gap(m, v) for m, v in zip(means, variances)]

        means_clean = [m if m is not None else float("nan") for m in means]
        vars_clean  = [v if v is not None else float("nan") for v in variances]
        gaps_clean  = [g if g is not None else float("nan") for g in gaps]
        cis_arr     = np.array([c if c is not None else float("nan") for c in cis])
        means_arr   = np.array(means_clean)

        crossover_row, _ = find_crossover(rows, mean_key, var_key, tolerance_pct)
        crossover_n = crossover_row["n_runs"] if crossover_row else None

        # Left panel: mean and variance
        ax_left = fig.add_subplot(gs[panel_idx, 0])
        ax_left.plot(n_values, means_clean, "o-",
                     color="#1565C0", linewidth=1.5, markersize=4,
                     label="Mean")
        ax_left.plot(n_values, vars_clean, "s--",
                     color="#C62828", linewidth=1.5, markersize=4,
                     label="Variance")
        ax_left.fill_between(
            n_values,
            means_arr - cis_arr,
            means_arr + cis_arr,
            alpha=0.12, color="#1565C0", label="Mean ± CI95"
        )
        if crossover_n:
            ax_left.axvline(crossover_n, color="#2E7D32", linestyle=":",
                            linewidth=1.5,
                            label=f"Crossover n={crossover_n}")
        ax_left.set_title(label, fontsize=10)
        ax_left.set_ylabel("Value")
        ax_left.set_xlabel("Sample count (n)")
        ax_left.legend(fontsize=7)
        ax_left.grid(True, alpha=0.25)

        # Right panel: relative gap
        ax_right = fig.add_subplot(gs[panel_idx, 1])
        ax_right.plot(n_values, gaps_clean, "D-",
                      color="#6A1B9A", linewidth=1.5, markersize=4,
                      label="Relative gap %")
        ax_right.axhline(tolerance_pct, color="#B71C1C", linestyle="--",
                         linewidth=1, label=f"Tolerance {tolerance_pct}%")
        if crossover_n:
            ax_right.axvline(crossover_n, color="#2E7D32", linestyle=":",
                             linewidth=1.5)
        ax_right.set_title(f"{label} — gap", fontsize=10)
        ax_right.set_ylabel("|mean − variance| / max (%)  ")
        ax_right.set_xlabel("Sample count (n)")
        ax_right.legend(fontsize=7)
        ax_right.grid(True, alpha=0.25)

    os.makedirs(out_dir, exist_ok=True)
    safe_scenario = scenario.replace("/", "_").replace(" ", "_")
    out_path = os.path.join(out_dir, f"receiver_convergence_{safe_scenario}.png")
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"\nPlot saved: {out_path}")
    return out_path


def main():
    parser = argparse.ArgumentParser(
        description="Find receiver-side statistical significance crossover "
                    "from accumulated scenario_statistics history."
    )
    parser.add_argument(
        "--receiver-db", default=config.DB_PATH,
        help="Path to receiver SQLite DB."
    )
    parser.add_argument(
        "--scenario", default=None,
        help="Filter to a specific scenario (e.g., los, nlos, los_link_failure). "
             "If omitted, all scenarios are analysed."
    )
    parser.add_argument(
        "--tolerance", type=float, default=10.0,
        help="Relative gap tolerance in percent for mean≈variance crossover "
             "(default: 10.0)."
    )
    parser.add_argument(
        "--out-dir", default=os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "modeling_reports", "statistical_reports"
        ),
        help="Output directory for convergence plots."
    )
    args = parser.parse_args()

    if not os.path.exists(args.receiver_db):
        sys.exit(f"Receiver DB not found: {args.receiver_db}")

    all_rows = fetch_scenario_statistics_history(
        db_path=args.receiver_db,
        scenario=args.scenario
    )

    if not all_rows:
        sys.exit(
            "No scenario_statistics history found. "
            "Run receiver_report.py at least once per transfer batch first."
        )

    # Group by scenario
    by_scenario = defaultdict(list)
    for row in all_rows:
        by_scenario[row["scenario"]].append(row)

    generated_plots = []
    for scenario_name, rows in sorted(by_scenario.items()):
        # Sort by sample count ascending
        rows_sorted = sorted(rows, key=lambda r: r["n_runs"])
        print_scenario_table(scenario_name, rows_sorted, args.tolerance)
        plot_path = plot_scenario(
            scenario_name, rows_sorted, args.tolerance, args.out_dir
        )
        generated_plots.append(plot_path)

    print(f"\n{len(generated_plots)} convergence plot(s) generated.")


if __name__ == "__main__":
    main()
