#!/usr/bin/env python3
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
from db_utils import fetch_receiver_checkpoint_history


METRICS = [
    ("chunk_to_chunk", "ctc", "Chunk-to-chunk time (s)"),
    ("file_to_file", "ftf", "File-to-file time (s)"),
    ("goodput", "gp", "Goodput (Mbps)"),
]


def relative_gap(value_a, value_b):
    if value_a is None or value_b is None:
        return None
    denom = max(abs(value_a), abs(value_b), 1e-9)
    return abs(value_a - value_b) / denom * 100.0


def metric_distance(row, prefix, criterion):
    """Return distance score in percent for the selected convergence criterion."""
    mean_val = row.get(f"{prefix}_mean")
    std_val = row.get(f"{prefix}_std")
    variance_val = row.get(f"{prefix}_variance")

    if criterion == "mean-variance":
        return relative_gap(mean_val, variance_val)

    if criterion == "mean-std":
        return relative_gap(mean_val, std_val)

    if criterion == "cv":
        if mean_val is None or std_val is None:
            return None
        denom = max(abs(mean_val), 1e-9)
        return abs(std_val) / denom * 100.0

    return None


def find_crossover(rows, prefix, tolerance_pct, criterion):
    """
    Return the first row where rel_gap(mean, variance) <= tolerance_pct.
    """
    for row in rows:
        gap = metric_distance(row, prefix, criterion)
        if gap is not None and gap <= tolerance_pct:
            return row, gap
    return None, None


def find_nearest_convergence(rows, prefix, criterion):
    """Return the row with the smallest relative gap, even if above tolerance."""
    best_row = None
    best_gap = None
    for row in rows:
        gap = metric_distance(row, prefix, criterion)
        if gap is None:
            continue
        if best_gap is None or gap < best_gap:
            best_row = row
            best_gap = gap
    return best_row, best_gap


def combine_checkpoint_rows(rows):
    """Combine per-metric checkpoint rows into one row per file_count."""
    combined = defaultdict(dict)
    metric_prefix = {
        "chunk_to_chunk": "ctc",
        "file_to_file": "ftf",
        "goodput": "gp",
    }

    for row in rows:
        file_count = int(row["file_count"])
        metric = str(row["metric"])
        prefix = metric_prefix.get(metric)
        if prefix is None:
            continue

        entry = combined[file_count]
        entry["n_runs"] = file_count
        entry["report_count"] = int(row.get("report_count") or 0)
        entry["sample_count"] = int(row.get("sample_count") or 0)
        entry[f"{prefix}_mean"] = row.get("mean_value")
        entry[f"{prefix}_variance"] = row.get("variance_value")
        entry[f"{prefix}_std"] = row.get("std_value")
        entry[f"{prefix}_ci95"] = row.get("ci95_value")

    return [combined[k] for k in sorted(combined)]


def print_scenario_table(scenario, rows, tolerance_pct, criterion):
    print(f"\n{'='*70}")
    print(f"Scenario: {scenario}  (tolerance = {tolerance_pct}%, criterion = {criterion})")
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
    for _, prefix, label in METRICS:
        crossover_row, gap = find_crossover(rows, prefix, tolerance_pct, criterion)
        if crossover_row:
            print(
                f"\n[{label}] crossover at n={crossover_row['n_runs']} "
                f"(gap={gap:.2f}%)"
            )
        else:
            nearest_row, nearest_gap = find_nearest_convergence(rows, prefix, criterion)
            if nearest_row:
                print(
                    f"\n[{label}] no crossover within {tolerance_pct}% tolerance; "
                    f"nearest point at n={nearest_row['n_runs']} (gap={nearest_gap:.2f}%)"
                )
            else:
                print(f"\n[{label}] no valid convergence points available")


def plot_scenario(scenario, rows, tolerance_pct, out_dir, criterion):
    n_values = [row["n_runs"] for row in rows]

    fig = plt.figure(figsize=(14, 11))
    fig.suptitle(
        f"Convergence analysis — scenario: {scenario}\n"
        f"(tolerance = {tolerance_pct}%, criterion = {criterion})",
        fontsize=13,
        y=0.98
    )

    gs = gridspec.GridSpec(3, 2, figure=fig, hspace=0.45, wspace=0.35)

    for panel_idx, (_, prefix, label) in enumerate(METRICS):
        mean_key = f"{prefix}_mean"
        var_key = f"{prefix}_variance"
        ci_key = f"{prefix}_ci95"
        means     = [row.get(mean_key) for row in rows]
        variances = [row.get(var_key)  for row in rows]
        cis       = [row.get(ci_key)   for row in rows]
        gaps      = [metric_distance(row, prefix, criterion) for row in rows]

        means_clean = [m if m is not None else float("nan") for m in means]
        vars_clean  = [v if v is not None else float("nan") for v in variances]
        gaps_clean  = [g if g is not None else float("nan") for g in gaps]
        cis_arr     = np.array([c if c is not None else float("nan") for c in cis])
        means_arr   = np.array(means_clean)

        crossover_row, _ = find_crossover(rows, prefix, tolerance_pct, criterion)
        crossover_n = crossover_row["n_runs"] if crossover_row else None
        nearest_row, _ = find_nearest_convergence(rows, prefix, criterion)
        nearest_n = nearest_row["n_runs"] if nearest_row else None

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
        elif nearest_n:
            ax_left.axvline(nearest_n, color="#EF6C00", linestyle=":",
                            linewidth=1.5,
                            label=f"Nearest n={nearest_n}")
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
        elif nearest_n:
            ax_right.axvline(nearest_n, color="#EF6C00", linestyle=":",
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
        "--criterion",
        choices=["mean-variance", "mean-std", "cv"],
        default="mean-std",
        help="Convergence criterion: mean-variance (legacy), mean-std (unit-consistent), or cv (std/mean%%).",
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

    all_rows = fetch_receiver_checkpoint_history(
        db_path=args.receiver_db,
        scenario=args.scenario
    )
    if not all_rows:
        sys.exit(
            "No checkpoint history found. "
            "Run generate_statistical_report.py at least once per transfer batch first."
        )
    by_scenario = defaultdict(list)
    for row in all_rows:
        by_scenario[row["scenario"]].append(row)

    generated_plots = []
    for scenario_name, rows in sorted(by_scenario.items()):
        rows_sorted = combine_checkpoint_rows(rows)
        print_scenario_table(scenario_name, rows_sorted, args.tolerance, args.criterion)
        plot_path = plot_scenario(
            scenario_name, rows_sorted, args.tolerance, args.out_dir, args.criterion
        )
        generated_plots.append(plot_path)

    print(f"\n{len(generated_plots)} convergence plot(s) generated.")


if __name__ == "__main__":
    main()
