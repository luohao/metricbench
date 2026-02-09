#!/usr/bin/env python3
"""
Generates all SQL queries for both on-demand and pre-agg approaches.
Reads experiment and metric configs, renders Jinja2 templates, writes SQL files.

Usage:
    python -m benchmark.generate_queries [--output /tmp/experimentation-benchmark/queries]
"""

import argparse
import json
import math
import os
from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader


def load_configs(config_dir: str) -> tuple:
    """Load experiment and metric configurations."""
    with open(os.path.join(config_dir, "experiments.yaml")) as f:
        exp_config = yaml.safe_load(f)
    with open(os.path.join(config_dir, "metrics.yaml")) as f:
        met_config = yaml.safe_load(f)
    return exp_config, met_config


def build_conversion_window_clause(exp: dict, unit_alias: str, metric_alias: str) -> str:
    """Build the timestamp comparison for conversion windows (on-demand approach)."""
    delay_hours = exp.get("delay_hours", 0)
    window_hours = exp.get("conversion_window_hours", 72)
    window_type = exp.get("window_type", "conversion")
    attribution = exp.get("attribution", "first_exposure")
    end_date = exp.get("end_date", "2022-02-01T00:00:00")

    base_col = f"{unit_alias}.first_exposure_timestamp"
    metric_col = f"{metric_alias}.timestamp"

    # Start of window
    if delay_hours < 0:
        start = f"{metric_col} >= {base_col} - INTERVAL '{abs(delay_hours)} hours'"
    elif delay_hours > 0:
        start = f"{metric_col} >= {base_col} + INTERVAL '{delay_hours} hours'"
    else:
        start = f"{metric_col} >= {base_col}"

    # End of window
    if attribution == "experiment_duration":
        end = f"{metric_col} <= '{end_date}'"
    else:
        total_hours = delay_hours + window_hours
        if total_hours >= 0:
            end = f"{metric_col} <= {base_col} + INTERVAL '{total_hours} hours'"
        else:
            end = f"{metric_col} <= {base_col} - INTERVAL '{abs(total_hours)} hours'"

    clause = f"{start}\n           AND {end}"

    # Lookback window: additional constraint
    if window_type == "lookback":
        lookback_hours = abs(window_hours)
        clause += (
            f"\n           AND {metric_col} + INTERVAL '{lookback_hours} hours' >= '{end_date}'"
        )

    return clause


def build_preagg_window_clause(exp: dict) -> str:
    """Build the date-level window clause for pre-aggregated approach."""
    delay_hours = exp.get("delay_hours", 0)
    window_hours = exp.get("conversion_window_hours", 72)
    window_type = exp.get("window_type", "conversion")
    attribution = exp.get("attribution", "first_exposure")
    end_date = exp.get("end_date", "2022-02-01T00:00:00")

    delay_days = math.floor(delay_hours / 24) if delay_hours < 0 else math.ceil(delay_hours / 24)
    window_days = math.ceil(window_hours / 24)

    # Start of window
    if delay_days < 0:
        start = f"m.metric_date >= CAST(u.first_exposure - INTERVAL '{abs(delay_days)} days' AS DATE)"
    elif delay_days > 0:
        start = f"m.metric_date >= CAST(u.first_exposure + INTERVAL '{delay_days} days' AS DATE)"
    else:
        start = "m.metric_date >= CAST(u.first_exposure AS DATE)"

    # End of window
    if attribution == "experiment_duration":
        end = f"m.metric_date <= '{end_date}'::date"
    else:
        total_days = delay_days + window_days
        if total_days >= 0:
            end = f"m.metric_date <= CAST(u.first_exposure + INTERVAL '{total_days} days' AS DATE)"
        else:
            end = f"m.metric_date <= CAST(u.first_exposure - INTERVAL '{abs(total_days)} days' AS DATE)"

    clause = f"{start}\n    AND {end}"

    # Lookback
    if window_type == "lookback":
        clause += f"\n    AND m.metric_date + {window_days} >= '{end_date}'::date"

    return clause


def build_preagg_sketch_window_clause(exp: dict) -> str:
    """Same as preagg window but for sketch tables (uses s. alias)."""
    clause = build_preagg_window_clause(exp)
    return clause.replace("m.metric_date", "s.metric_date")


def generate_query(
    env: Environment,
    approach: str,
    exp: dict,
    defaults: dict,
    metric: dict,
    variant: str = "standard",
    use_tdigest: bool = False,
    use_approx_quantile: bool = False,
) -> str:
    """Generate a single SQL query from templates."""
    # Merge experiment with defaults
    merged_exp = {**defaults, **exp}

    # Build units CTE
    units_template = env.get_template(f"{approach}/units.sql.j2")
    units_cte = units_template.render(
        exposure_table=merged_exp.get("exposure_table", "viewed_experiment"),
        experiment_id=merged_exp["experiment_id"],
        start_date=merged_exp["start_date"],
        end_date=merged_exp["end_date"],
        conversion_window_hours=merged_exp.get("conversion_window_hours", 72),
        activation=merged_exp.get("activation"),
        activation_end_date=merged_exp["end_date"],
        segment=merged_exp.get("segment"),
        dimension=merged_exp.get("dimension"),
        identity_join=merged_exp.get("activation", {}).get("identity_join")
        if merged_exp.get("activation")
        else None,
    )

    # Build conversion window clause
    if approach == "ondemand":
        conv_clause = build_conversion_window_clause(merged_exp, "u", "m")

        def conversion_window_clause(u_alias, m_alias):
            return build_conversion_window_clause(merged_exp, u_alias, m_alias)
    else:
        conv_clause = build_preagg_window_clause(merged_exp)

    # Determine CUPED and capping
    cuped = metric.get("cuped") if metric.get("cuped", {}).get("enabled") else None
    capping = metric.get("capping")

    # Determine weighting
    weighted = variant == "weighted"

    # Build metric query
    metric_template = env.get_template(f"{approach}/metric.sql.j2")

    dimension = merged_exp.get("dimension")
    dimension_is_activation = (
        dimension and dimension.get("type") == "activation"
    )

    rendered = metric_template.render(
        units_cte=units_cte,
        metric=metric,
        experiment_id=merged_exp["experiment_id"],
        start_date=merged_exp["start_date"],
        end_date=merged_exp["end_date"],
        conversion_window_hours=merged_exp.get("conversion_window_hours", 72),
        conversion_window_clause=conversion_window_clause
        if approach == "ondemand"
        else None,
        preagg_window_clause=build_preagg_window_clause(merged_exp)
        if approach == "preagg"
        else None,
        preagg_window_clause_sketch=build_preagg_sketch_window_clause(merged_exp)
        if approach == "preagg"
        else None,
        activation=merged_exp.get("activation"),
        dimension=dimension,
        dimension_is_activation=dimension_is_activation,
        segment=merged_exp.get("segment"),
        skip_partial_data=merged_exp.get("skip_partial_data", False),
        cuped=cuped,
        capping=capping,
        weighted=weighted,
        use_tdigest=use_tdigest,
        use_approx_quantile=use_approx_quantile,
    )

    return rendered


def main():
    parser = argparse.ArgumentParser(description="Generate benchmark SQL queries")
    parser.add_argument(
        "--config-dir",
        default="configs",
        help="Directory containing experiment and metric YAML configs",
    )
    parser.add_argument(
        "--template-dir",
        default="templates",
        help="Directory containing Jinja2 SQL templates",
    )
    parser.add_argument(
        "--output",
        default="/tmp/experimentation-benchmark/queries",
        help="Output directory for generated SQL files",
    )
    parser.add_argument(
        "--experiments",
        default=None,
        help="Comma-separated experiment IDs to generate (default: all)",
    )
    parser.add_argument(
        "--metrics",
        default=None,
        help="Comma-separated metric IDs to generate (default: all)",
    )
    parser.add_argument(
        "--approx-quantile",
        action="store_true",
        help="Use DuckDB approx_quantile for quantile metrics (built-in T-Digest)",
    )
    args = parser.parse_args()

    # Load configs
    exp_config, met_config = load_configs(args.config_dir)
    defaults = exp_config.get("defaults", {})
    experiments = exp_config["experiments"]
    metrics = met_config["metrics"]

    # Filter if specified
    if args.experiments:
        exp_ids = set(args.experiments.split(","))
        experiments = [e for e in experiments if e["id"] in exp_ids]
    if args.metrics:
        met_ids = set(args.metrics.split(","))
        metrics = [m for m in metrics if m["id"] in met_ids]

    # Set up Jinja2
    env = Environment(
        loader=FileSystemLoader(args.template_dir),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    # Output directories
    for approach in ["ondemand", "preagg"]:
        Path(os.path.join(args.output, approach)).mkdir(parents=True, exist_ok=True)

    # Generate all queries
    query_manifest = []
    total = 0

    for exp in experiments:
        for metric in metrics:
            # On-demand approach: one query per experiment x metric
            try:
                sql = generate_query(
                    env, "ondemand", exp, defaults, metric,
                    use_approx_quantile=args.approx_quantile,
                )
                filename = f"ondemand/{exp['id']}__{metric['id']}.sql"
                filepath = os.path.join(args.output, filename)
                with open(filepath, "w") as f:
                    f.write(sql)
                query_manifest.append(
                    {
                        "experiment": exp["id"],
                        "metric": metric["id"],
                        "approach": "ondemand",
                        "variant": "standard",
                        "file": filename,
                    }
                )
                total += 1
            except Exception as e:
                print(f"  WARN: ondemand/{exp['id']}__{metric['id']}: {e}")

            # Pre-agg approach: unweighted variant
            for variant in ["unweighted", "weighted"]:
                try:
                    sql = generate_query(
                        env, "preagg", exp, defaults, metric, variant=variant,
                        use_approx_quantile=args.approx_quantile,
                    )
                    filename = f"preagg/{exp['id']}__{metric['id']}__{variant}.sql"
                    filepath = os.path.join(args.output, filename)
                    with open(filepath, "w") as f:
                        f.write(sql)
                    query_manifest.append(
                        {
                            "experiment": exp["id"],
                            "metric": metric["id"],
                            "approach": "preagg",
                            "variant": variant,
                            "file": filename,
                        }
                    )
                    total += 1
                except Exception as e:
                    print(f"  WARN: preagg/{exp['id']}__{metric['id']}__{variant}: {e}")

    # Write manifest
    manifest_path = os.path.join(args.output, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(query_manifest, f, indent=2)

    print(f"Generated {total} SQL files in {args.output}/")
    print(f"  On-demand: {sum(1 for q in query_manifest if q['approach'] == 'ondemand')}")
    print(f"  Pre-agg:   {sum(1 for q in query_manifest if q['approach'] == 'preagg')}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
