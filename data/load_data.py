#!/usr/bin/env python3
"""
Load generated CSV data into the target database and optionally build pre-agg tables.

Usage:
    python -m data.load_data [--config config.yaml] [--csv-dir /tmp/.../csv] [--preagg]

Supports both DuckDB (default) and Postgres engines.
"""

import argparse
import os
import subprocess
import sys
import time

import yaml


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def run_psql(config: dict, sql_file: str, description: str):
    """Run a SQL file via psql."""
    pg = config["postgres"]
    env = os.environ.copy()
    if pg.get("password"):
        env["PGPASSWORD"] = pg["password"]

    cmd = [
        "psql",
        "-h", pg.get("host", "localhost"),
        "-p", str(pg.get("port", 5432)),
        "-U", pg.get("user", "postgres"),
        "-d", pg["database"],
        "-f", sql_file,
    ]

    print(f"\n{description}...")
    start = time.time()
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    elapsed = time.time() - start

    if result.returncode != 0:
        print(f"  ERROR: {result.stderr}")
        sys.exit(1)
    else:
        print(f"  Done in {elapsed:.2f}s")
        if result.stdout:
            # Print last few lines of output
            lines = result.stdout.strip().split("\n")
            for line in lines[-5:]:
                print(f"    {line}")


def run_duckdb_sql_file(config: dict, sql_file: str, description: str):
    """Execute a SQL file using DuckDB."""
    import duckdb

    db_path = config["duckdb"].get("database", ":memory:")
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    print(f"\n{description}...")
    start = time.time()

    conn = duckdb.connect(db_path)
    with open(sql_file) as f:
        sql = f.read()

    # Execute each statement separately (DuckDB requires single-statement execution)
    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            conn.execute(stmt)

    elapsed = time.time() - start
    conn.close()
    print(f"  Done in {elapsed:.2f}s")


def main():
    parser = argparse.ArgumentParser(description="Load benchmark data into database")
    parser.add_argument("--config", default="config.yaml", help="Config file path")
    parser.add_argument(
        "--engine",
        choices=["duckdb", "postgres"],
        default=None,
        help="Database engine (default: from config, or duckdb)",
    )
    parser.add_argument(
        "--csv-dir",
        default="/tmp/experimentation-benchmark/csv",
        help="Directory with CSV files",
    )
    parser.add_argument(
        "--schemas-dir", default="schemas", help="Directory with SQL schema files"
    )
    parser.add_argument(
        "--preagg",
        action="store_true",
        help="Also build pre-aggregated tables",
    )
    parser.add_argument(
        "--tdigest",
        action="store_true",
        help="Also build t-digest sketch tables (requires pg_tdigest extension)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    engine = args.engine or config.get("engine", "duckdb")

    # Check CSV files exist
    expected_files = [
        "exposures.csv",
        "orders.csv",
        "events.csv",
        "pages.csv",
        "sessions.csv",
    ]
    for f in expected_files:
        path = os.path.join(args.csv_dir, f)
        if not os.path.exists(path):
            print(f"ERROR: Missing CSV file: {path}")
            print("Run 'python -m data.generate_data' first.")
            sys.exit(1)

    if engine == "duckdb":
        raw_sql = os.path.join(args.schemas_dir, "duckdb", "raw_tables.sql")
        run_duckdb_sql_file(config, raw_sql, "Loading raw tables into DuckDB")

        if args.preagg:
            preagg_sql = os.path.join(args.schemas_dir, "duckdb", "preagg_tables.sql")
            run_duckdb_sql_file(
                config, preagg_sql, "Building pre-aggregated tables in DuckDB"
            )

        if args.tdigest:
            print("  NOTE: t-digest is not supported in DuckDB; skipping.")

        db_path = config["duckdb"].get("database", ":memory:")
        print(f"\nData loading complete! DuckDB database: {db_path}")

    elif engine == "postgres":
        raw_sql = os.path.join(args.schemas_dir, "postgres", "raw_tables.sql")
        run_psql(config, raw_sql, "Loading raw tables into Postgres")

        if args.preagg:
            preagg_sql = os.path.join(args.schemas_dir, "postgres", "preagg_tables.sql")
            run_psql(config, preagg_sql, "Building pre-aggregated tables in Postgres")

        if args.tdigest:
            tdigest_sql = os.path.join(
                args.schemas_dir, "postgres", "preagg_sketches_tdigest.sql"
            )
            run_psql(
                config, tdigest_sql, "Building t-digest sketch tables in Postgres"
            )

        print("\nData loading complete!")


if __name__ == "__main__":
    main()
