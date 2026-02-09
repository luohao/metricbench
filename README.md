# MetricBench: Experimentation SQL Benchmark

A benchmark for A/B test analysis, ported from [GrowthBook's integration tests](https://github.com/growthbook/growthbook/tree/main/packages/back-end/test/integrations/), comparing two architectural approaches:

1. **On-demand aggregation** -- scan raw event tables per experiment (how most platforms start)
2. **Pre-aggregation** -- shared daily tables built once, lightweight joins per experiment (how FAANG-scale systems work)

Use this to evaluate storage systems for experimentation platforms.

## Quick Start

```bash
# 1. Install dependencies
pip install -e .

# 2. Generate synthetic data (5,000 users, 120 days)
python -m data.generate_data --users 5000 --days 120

# 3. Load data into DuckDB (default engine, no server needed)
python -m data.load_data --preagg

# 4. Generate SQL queries for both approaches
python -m benchmark.generate_queries

# 5. Run the benchmark
python -m benchmark.run_benchmark --validate
```

Results are written to `/tmp/experimentation-benchmark/results/benchmark_results.json`.

### Using Postgres Instead

```bash
# Create a Postgres database
createdb experimentation_benchmark

# Install the Postgres driver
pip install psycopg2-binary

# Load data and run benchmark with --engine postgres
python -m data.load_data --engine postgres --preagg
python -m benchmark.run_benchmark --engine postgres --validate
```

## Engines

| Engine | Default | Setup | Notes |
|---|---|---|---|
| **DuckDB** | Yes | `pip install -e .` | Embedded, no server required. Single-file database. |
| **Postgres** | No | Requires running server | `pip install psycopg2-binary` + `createdb` |

The engine is configured in `config.yaml` (field `engine: duckdb`) and can be overridden per command with `--engine duckdb` or `--engine postgres`.

## What It Tests

### 22 Experiment Configurations

Covering all major experiment analysis features:

| Feature | Configs |
|---|---|
| First-exposure attribution | base, activation, dimensions, segments, ... |
| Experiment-duration attribution | base_allExposures, lookback_allExposures, ... |
| Activation metrics | activation, activation_anonymous, dimension_activation, ... |
| Negative conversion delay | negativeconversion, activation_negativeconversion, ... |
| Positive conversion delay | positiveconversion, activation_positiveconversion, ... |
| Lookback window | lookback, lookback_allExposures, lookback_skipPartial |
| Skip partial data | skipPartialData, lookback_skipPartial |
| Dimensions | dimension_date, dimension_experiment, dimension_user, dimension_activation |
| Segments | filter_segment |

### 33 Metric Definitions

| Type | Count | Examples |
|---|---|---|
| Binomial | 2 (+2 CUPED) | any_item_in_cart, any_item_in_cart_cuped |
| Count/Mean | 7 (+3 CUPED) | purchased_items, purchased_value, count_distinct_date |
| Percentile-capped | 4 | purchased_value_pctilecap, purchased_value_pctilecap_ignorezeros |
| Ratio | 6 | revenue_over_items, purchase_over_cart, chained ratio |
| Quantile | 4 | purchased_value_p90 (event/unit level, with/without zeros) |

### Two Approaches

**On-demand (per experiment):**
- Scans raw `viewed_experiment` table to build units CTE
- Scans raw `orders`/`events` tables for each metric
- Full timestamp precision for conversion windows

**Pre-aggregation (shared tables):**
- `shared_exposures` -- one row per (user, experiment, variation, date)
- `shared_metrics_daily` -- one row per (user, date) with all metric columns
- `shared_activations` -- activation events per user per day
- `shared_sketches_array` -- array columns for quantile metrics
- Date-level granularity (conversion windows rounded to days)
- Optional partial-day weighting

## Architecture

```
Raw Tables (5)                 Shared Tables (4)
  viewed_experiment    ──►     shared_exposures
  orders               ──►     shared_metrics_daily
  events               ──►     shared_activations
  pages                        shared_sketches_array
  sessions

On-demand approach:            Pre-agg approach:
  Per experiment:                Pipeline (once):
    Scan raw exposure table        Build 4 shared tables
    For each metric:             Per experiment:
      Scan raw metric table        Light join on shared tables
      JOIN with units              (no raw table scanning)
```

## Scaling the Benchmark

```bash
# Small (quick test): 1K users
python -m data.generate_data --users 1000

# Medium (default): 5K users
python -m data.generate_data --users 5000

# Large (realistic): 100K users
python -m data.generate_data --users 100000

# Extreme: 1M users
python -m data.generate_data --users 1000000
```

The on-demand approach scales linearly with data size (more raw rows to scan).
The pre-agg approach scales with the number of experiments (pipeline cost is fixed).

## Partial-Day Weighting

The pre-agg approach uses daily granularity, which introduces the "noon exposure problem": if a user is exposed at noon, the full day's events are included (not just post-exposure).

Two variants are benchmarked:
- **Unweighted**: Full day included (over-counts by ~0.5 days on average)
- **Weighted**: First day weighted by `(24 - hour_of_exposure) / 24`

## Approximate Quantiles (T-Digest)

Quantile metrics support three modes for the pre-agg approach:

| Mode | Engine | How | Flag |
|---|---|---|---|
| **Exact** (default) | Any | `ARRAY_AGG` + `UNNEST` + `PERCENTILE_CONT` | _(none)_ |
| **Approximate** | DuckDB | Built-in [`approx_quantile`](https://duckdb.org/docs/stable/sql/functions/aggregates#approximate-aggregates) (T-Digest) | `--approx-quantile` |
| **T-digest sketches** | Postgres | `pg_tdigest` extension with pre-computed sketches | `--tdigest` |

### DuckDB (recommended)

DuckDB's `approx_quantile` uses T-Digest internally -- no extension needed:

```bash
python -m benchmark.generate_queries --approx-quantile
python -m benchmark.run_benchmark --approach preagg
```

### Postgres

For Postgres, install the `pg_tdigest` extension for pre-computed sketch tables:

```bash
# Install pg_tdigest (varies by OS), then:
python -m data.load_data --engine postgres --tdigest
python -m benchmark.run_benchmark --engine postgres --approach preagg
```

## Output Format

```json
{
  "engine": "duckdb",
  "pipeline_timings": {
    "shared_exposures": 0.8,
    "shared_metrics_daily": 1.2,
    "shared_activations": 0.3,
    "shared_sketches_array": 0.5
  },
  "summary": {
    "ondemand_total_seconds": 45.2,
    "preagg_total_seconds": 12.5,
    "pipeline_total_seconds": 2.8,
    "speedup_analysis_only": "3.6x",
    "speedup_including_pipeline": "3.0x"
  },
  "queries": [...]
}
```

## Adding a New Engine

1. Create `schemas/<engine>/raw_tables.sql` and `preagg_tables.sql`
2. Create `benchmark/engines/<engine>.py` implementing `connect()`, `execute()`, `execute_query()`
3. Add engine-specific Jinja2 template overrides in `templates/` if needed (e.g., date functions)
4. Register the engine in `benchmark/run_benchmark.py:create_engine()` and `data/load_data.py`
5. Run: `python -m benchmark.run_benchmark --engine <engine>`

## Project Structure

```
experimentation-benchmark/
  config.yaml                   # DB connection settings (engine, duckdb, postgres)
  pyproject.toml                # Python dependencies
  configs/
    experiments.yaml            # 22 experiment configurations
    metrics.yaml                # 33 metric definitions
  data/
    generate_data.py            # Synthetic data generator
    load_data.py                # Loads CSVs into database (--engine duckdb|postgres)
  schemas/
    duckdb/
      raw_tables.sql            # 5 raw tables DDL + COPY
      preagg_tables.sql         # 4 shared tables DDL
    postgres/
      raw_tables.sql            # 5 raw tables DDL + \copy
      preagg_tables.sql         # 4 shared tables DDL
      preagg_sketches_tdigest.sql  # T-digest tables (optional)
  templates/
    ondemand/
      units.sql.j2              # Units CTE from raw tables
      metric.sql.j2             # Metric query from raw tables
    preagg/
      units.sql.j2              # Units CTE from shared tables
      metric.sql.j2             # Metric query from shared tables
  benchmark/
    generate_queries.py         # Renders templates into SQL files
    run_benchmark.py            # Executes and times both approaches
    engines/
      duckdb.py                 # DuckDB connection + execution (default)
      postgres.py               # Postgres connection + execution
```
