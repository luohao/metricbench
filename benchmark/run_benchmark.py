#!/usr/bin/env python3
"""
Benchmark runner: executes both on-demand and pre-agg approaches, times each query,
and outputs a comparison JSON.

Usage:
    python -m benchmark.run_benchmark [OPTIONS]

Options:
    --config config.yaml          Config file with DB connection settings
    --engine duckdb|postgres      Database engine (default: from config, or duckdb)
    --approach ondemand|preagg|both  Which approach(es) to benchmark (default: both)
    --queries /tmp/.../queries    Directory with generated SQL files
    --experiments base,activation Comma-separated experiment filter
    --metrics purchased_items,... Comma-separated metric filter
    --validate                    Compare results between approaches
    --output results.json         Output file path
    --warmup 1                    Number of warmup runs (not timed)
    --runs 3                      Number of timed runs (takes median)
"""

import argparse
import json
import os
import statistics
import time
from datetime import datetime
from pathlib import Path

import yaml


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def create_engine(engine_name: str, config: dict):
    """Create and return a database engine instance."""
    if engine_name == "duckdb":
        from benchmark.engines.duckdb import DuckDBEngine

        return DuckDBEngine(config["duckdb"])
    elif engine_name == "postgres":
        from benchmark.engines.postgres import PostgresEngine

        return PostgresEngine(config["postgres"])
    else:
        raise ValueError(f"Unknown engine: {engine_name}")


def run_preagg_pipeline(engine, schemas_dir: str, engine_name: str) -> dict:
    """Build all pre-aggregated tables and return per-table timings."""
    print("\n=== Building Pre-Aggregation Pipeline ===")
    preagg_file = os.path.join(schemas_dir, engine_name, "preagg_tables.sql")

    with open(preagg_file) as f:
        full_sql = f.read()

    # Split into individual statements and execute them
    timings = {}
    current_name = None
    statements = []

    for line in full_sql.split("\n"):
        stripped = line.strip()

        # Track which table we're building
        if stripped.upper().startswith("DROP TABLE IF EXISTS"):
            # Execute previous batch if any
            if current_name and statements:
                sql = "\n".join(statements)
                start = time.time()
                try:
                    engine.execute(sql)
                    timings[current_name] = time.time() - start
                    print(f"  {current_name}: {timings[current_name]:.3f}s")
                except Exception as e:
                    print(f"  {current_name}: ERROR - {e}")
                    timings[current_name] = -1
                statements = []

            # Extract table name (DROP TABLE IF EXISTS <name> [CASCADE];)
            parts = stripped.split()
            if len(parts) >= 5:
                current_name = parts[4].rstrip(";").replace("CASCADE", "").strip()
            statements.append(line)
        else:
            statements.append(line)

    # Execute last batch
    if current_name and statements:
        sql = "\n".join(statements)
        start = time.time()
        try:
            engine.execute(sql)
            timings[current_name] = time.time() - start
            print(f"  {current_name}: {timings[current_name]:.3f}s")
        except Exception as e:
            print(f"  {current_name}: ERROR - {e}")
            timings[current_name] = -1

    total = sum(t for t in timings.values() if t > 0)
    print(f"  TOTAL pipeline: {total:.3f}s")
    return timings


def run_queries(engine, queries_dir: str, manifest: list, warmup: int, runs: int) -> list:
    """Execute all queries in the manifest and return timed results."""
    results = []
    total = len(manifest)

    for i, entry in enumerate(manifest):
        filepath = os.path.join(queries_dir, entry["file"])
        if not os.path.exists(filepath):
            print(f"  SKIP [{i+1}/{total}] {entry['file']} (not found)")
            continue

        with open(filepath) as f:
            sql = f.read()

        name = f"{entry['approach']} > {entry['experiment']} > {entry['metric']}"
        if entry.get("variant") and entry["variant"] != "standard":
            name += f" > {entry['variant']}"

        # Warmup runs
        for _ in range(warmup):
            try:
                engine.execute_query(sql)
            except Exception:
                break

        # Timed runs
        timings = []
        last_result = None
        for r in range(runs):
            try:
                result = engine.execute_query(sql)
                timings.append(result["walltime_seconds"])
                last_result = result
            except Exception as e:
                print(f"  ERROR [{i+1}/{total}] {name}: {e}")
                timings.append(-1)

        valid_timings = [t for t in timings if t >= 0]
        median_time = statistics.median(valid_timings) if valid_timings else -1

        result_entry = {
            "experiment": entry["experiment"],
            "metric": entry["metric"],
            "approach": entry["approach"],
            "variant": entry.get("variant", "standard"),
            "walltime_seconds": round(median_time, 6),
            "all_timings": [round(t, 6) for t in timings],
            "row_count": last_result["row_count"] if last_result else 0,
            "rows": last_result["rows"][:5] if last_result else [],  # first 5 rows for validation
        }
        results.append(result_entry)

        if (i + 1) % 50 == 0 or i == 0:
            print(f"  [{i+1}/{total}] {name}: {median_time:.4f}s")

    return results


def compute_summary(results: list, pipeline_timings: dict) -> dict:
    """Compute summary statistics from benchmark results."""
    ondemand_times = [
        r["walltime_seconds"]
        for r in results
        if r["approach"] == "ondemand" and r["walltime_seconds"] > 0
    ]
    preagg_times = [
        r["walltime_seconds"]
        for r in results
        if r["approach"] == "preagg" and r["walltime_seconds"] > 0
    ]

    pipeline_total = sum(t for t in pipeline_timings.values() if t > 0)
    num_experiments = len(set(r["experiment"] for r in results))

    ondemand_total = sum(ondemand_times) if ondemand_times else 0
    preagg_total = sum(preagg_times) if preagg_times else 0

    return {
        "ondemand_query_count": len(ondemand_times),
        "ondemand_total_seconds": round(ondemand_total, 3),
        "ondemand_median_per_query": round(
            statistics.median(ondemand_times), 6
        )
        if ondemand_times
        else 0,
        "preagg_query_count": len(preagg_times),
        "preagg_total_seconds": round(preagg_total, 3),
        "preagg_median_per_query": round(
            statistics.median(preagg_times), 6
        )
        if preagg_times
        else 0,
        "pipeline_total_seconds": round(pipeline_total, 3),
        "pipeline_amortized_per_experiment": round(
            pipeline_total / num_experiments, 3
        )
        if num_experiments
        else 0,
        "preagg_total_with_pipeline": round(preagg_total + pipeline_total, 3),
        "speedup_analysis_only": f"{ondemand_total / preagg_total:.1f}x"
        if preagg_total > 0
        else "N/A",
        "speedup_including_pipeline": f"{ondemand_total / (preagg_total + pipeline_total):.1f}x"
        if (preagg_total + pipeline_total) > 0
        else "N/A",
    }


def _pct_diff(a: float, b: float) -> float:
    """Symmetric percentage difference between two values."""
    if a == 0 and b == 0:
        return 0.0
    denom = max(abs(a), abs(b))
    if denom == 0:
        return 0.0
    return abs(a - b) / denom * 100


def _extract_field_totals(rows: list, field: str) -> float:
    """Sum a field across all result rows (handles variation-level aggregation)."""
    total = 0.0
    for r in rows:
        if isinstance(r, dict) and r.get(field) is not None:
            try:
                total += float(r[field])
            except (TypeError, ValueError):
                pass
    return total


def validate_results(results: list) -> dict:
    """Compare on-demand vs pre-agg results for the same experiment x metric.

    For each pair, compares user counts and metric values (main_sum,
    denominator_sum, quantile_value) and reports the percentage difference.
    Comparisons are categorized by tolerance:
      - exact:  < 1%  (rounding only)
      - close:  1-10% (expected from daily-granularity approximation)
      - far:    > 10% (worth investigating)
    """
    # Fields to compare, in priority order (first match wins)
    COMPARE_FIELDS = ["users", "main_sum", "quantile_value"]

    # Group by experiment + metric
    grouped = {}
    for r in results:
        key = f"{r['experiment']}__{r['metric']}"
        if key not in grouped:
            grouped[key] = {}
        approach_key = f"{r['approach']}_{r.get('variant', 'standard')}"
        grouped[key][approach_key] = r

    comparisons = []
    skipped = 0

    for key, approaches in grouped.items():
        ondemand = approaches.get("ondemand_standard")

        # Compare against both unweighted and weighted preagg
        for variant_key, variant_label in [
            ("preagg_unweighted", "unweighted"),
            ("preagg_weighted", "weighted"),
        ]:
            preagg = approaches.get(variant_key)
            if not ondemand or not preagg:
                skipped += 1
                continue

            od_rows = ondemand.get("rows", [])
            pa_rows = preagg.get("rows", [])

            if not od_rows or not pa_rows:
                skipped += 1
                continue

            comp = {
                "key": key,
                "variant": variant_label,
                "diffs": {},
            }

            has_any_value = False
            for field in COMPARE_FIELDS:
                od_val = _extract_field_totals(od_rows, field)
                pa_val = _extract_field_totals(pa_rows, field)

                if od_val == 0 and pa_val == 0:
                    continue

                has_any_value = True
                diff = _pct_diff(od_val, pa_val)
                comp["diffs"][field] = {
                    "ondemand": round(od_val, 4),
                    "preagg": round(pa_val, 4),
                    "diff_pct": round(diff, 2),
                }

            if has_any_value:
                # Overall diff is the max across all compared fields
                max_diff = max(
                    d["diff_pct"] for d in comp["diffs"].values()
                )
                comp["max_diff_pct"] = round(max_diff, 2)
                comparisons.append(comp)
            else:
                skipped += 1

    # Categorize
    all_diffs = [c["max_diff_pct"] for c in comparisons]
    exact = sum(1 for d in all_diffs if d < 1)
    close = sum(1 for d in all_diffs if 1 <= d < 10)
    far = sum(1 for d in all_diffs if d >= 10)

    # Summary stats
    if all_diffs:
        sorted_diffs = sorted(all_diffs)
        p50_idx = len(sorted_diffs) // 2
        p95_idx = min(int(len(sorted_diffs) * 0.95), len(sorted_diffs) - 1)
        diff_stats = {
            "median_pct": round(sorted_diffs[p50_idx], 2),
            "p95_pct": round(sorted_diffs[p95_idx], 2),
            "max_pct": round(sorted_diffs[-1], 2),
        }
    else:
        diff_stats = {}

    # Top outliers (for debugging)
    outliers = sorted(comparisons, key=lambda c: c["max_diff_pct"], reverse=True)[:10]
    outlier_summary = [
        {
            "key": o["key"],
            "variant": o["variant"],
            "max_diff_pct": o["max_diff_pct"],
            "diffs": o["diffs"],
        }
        for o in outliers
        if o["max_diff_pct"] >= 1
    ]

    return {
        "total_comparisons": len(comparisons),
        "exact_lt_1pct": exact,
        "close_1_to_10pct": close,
        "far_gt_10pct": far,
        "skipped": skipped,
        "diff_stats": diff_stats,
        "top_outliers": outlier_summary,
    }


def main():
    parser = argparse.ArgumentParser(description="Run experimentation benchmark")
    parser.add_argument(
        "--config", default="config.yaml", help="Config file path"
    )
    parser.add_argument(
        "--engine",
        choices=["duckdb", "postgres"],
        default=None,
        help="Database engine (default: from config, or duckdb)",
    )
    parser.add_argument(
        "--approach",
        choices=["ondemand", "preagg", "both"],
        default="both",
        help="Which approach(es) to benchmark",
    )
    parser.add_argument(
        "--queries",
        default="/tmp/experimentation-benchmark/queries",
        help="Directory with generated SQL files",
    )
    parser.add_argument(
        "--schemas",
        default="schemas",
        help="Directory with schema SQL files",
    )
    parser.add_argument("--experiments", default=None, help="Filter experiments")
    parser.add_argument("--metrics", default=None, help="Filter metrics")
    parser.add_argument(
        "--validate", action="store_true", help="Compare results between approaches"
    )
    parser.add_argument(
        "--output",
        default="/tmp/experimentation-benchmark/results/benchmark_results.json",
        help="Output file path",
    )
    parser.add_argument("--warmup", type=int, default=1, help="Warmup runs")
    parser.add_argument("--runs", type=int, default=3, help="Timed runs")
    args = parser.parse_args()

    config = load_config(args.config)
    engine_name = args.engine or config.get("engine", "duckdb")

    # Load query manifest
    manifest_path = os.path.join(args.queries, "manifest.json")
    with open(manifest_path) as f:
        manifest = json.load(f)

    # Filter
    if args.experiments:
        exp_ids = set(args.experiments.split(","))
        manifest = [m for m in manifest if m["experiment"] in exp_ids]
    if args.metrics:
        met_ids = set(args.metrics.split(","))
        manifest = [m for m in manifest if m["metric"] in met_ids]
    if args.approach != "both":
        manifest = [m for m in manifest if m["approach"] == args.approach]

    print(f"Benchmark: {len(manifest)} queries to execute")
    print(f"  Engine: {engine_name}")
    print(f"  Warmup runs: {args.warmup}")
    print(f"  Timed runs: {args.runs}")

    # Connect to database
    engine = create_engine(engine_name, config)
    engine.connect()
    print(f"Connected to {engine_name}")

    # Run pre-agg pipeline (if needed)
    pipeline_timings = {}
    if args.approach in ("preagg", "both"):
        pipeline_timings = run_preagg_pipeline(engine, args.schemas, engine_name)

    # Run queries
    print(f"\n=== Running {len(manifest)} Queries ===")
    results = run_queries(engine, args.queries, manifest, args.warmup, args.runs)

    # Compute summary
    summary = compute_summary(results, pipeline_timings)

    # Validate if requested
    validation = {}
    if args.validate:
        print("\n=== Validating Results (on-demand vs pre-agg) ===")
        validation = validate_results(results)
        total = validation["total_comparisons"]
        print(f"  Comparisons:  {total}")
        print(
            f"  Exact (<1%):  {validation['exact_lt_1pct']}"
            f"  Close (1-10%): {validation['close_1_to_10pct']}"
            f"  Far (>10%):   {validation['far_gt_10pct']}"
        )
        if validation.get("diff_stats"):
            ds = validation["diff_stats"]
            print(
                f"  Diff distribution: median={ds['median_pct']}%"
                f"  p95={ds['p95_pct']}%  max={ds['max_pct']}%"
            )
        if validation.get("top_outliers"):
            print(f"  Top outliers (>= 1% diff):")
            for o in validation["top_outliers"][:5]:
                print(f"    {o['key']} ({o['variant']}): {o['max_diff_pct']}%")

    # Build output
    output = {
        "engine": engine_name,
        "timestamp": datetime.now().isoformat(),
        "config": {
            "warmup_runs": args.warmup,
            "timed_runs": args.runs,
        },
        "pipeline_timings": pipeline_timings,
        "summary": summary,
        "validation": validation,
        "queries": results,
    }

    # Write output
    Path(os.path.dirname(args.output)).mkdir(parents=True, exist_ok=True)
    with open(args.output, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n=== Summary ===")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    print(f"\nResults written to: {args.output}")

    engine.close()


if __name__ == "__main__":
    main()
