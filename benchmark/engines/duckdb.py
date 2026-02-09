"""DuckDB database engine for the benchmark."""

import time

import duckdb


class DuckDBEngine:
    """Manages DuckDB connections and query execution."""

    def __init__(self, config: dict):
        self.config = config
        self.conn = None

    def connect(self):
        db_path = self.config.get("database", ":memory:")
        self.conn = duckdb.connect(db_path)

    def close(self):
        if self.conn:
            self.conn.close()

    def execute_sql_file(self, filepath: str) -> float:
        """Execute a SQL file and return wall-clock time in seconds."""
        with open(filepath, "r") as f:
            sql = f.read()
        return self.execute(sql)

    def execute(self, sql: str) -> float:
        """Execute SQL (potentially multiple statements) and return wall-clock time."""
        start = time.time()
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for stmt in statements:
            self.conn.execute(stmt)
        elapsed = time.time() - start
        return elapsed

    def execute_query(self, sql: str) -> dict:
        """Execute a query and return timing + results."""
        start = time.time()
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        rows = []
        for stmt in statements:
            result = self.conn.execute(stmt)
            if result.description:
                columns = [desc[0] for desc in result.description]
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
        elapsed = time.time() - start
        return {
            "walltime_seconds": elapsed,
            "rows": rows,
            "row_count": len(rows),
        }

    def execute_preagg_pipeline(self, sql_file: str) -> dict:
        """Execute the pre-aggregation pipeline and return per-table timings."""
        with open(sql_file, "r") as f:
            full_sql = f.read()

        # Split by DROP/CREATE TABLE statements to time each table separately
        timings = {}
        current_table = None
        current_sql = []

        for line in full_sql.split("\n"):
            if line.strip().upper().startswith("DROP TABLE IF EXISTS"):
                # Save previous table's SQL
                if current_table and current_sql:
                    sql = "\n".join(current_sql)
                    start = time.time()
                    for stmt in sql.split(";"):
                        stmt = stmt.strip()
                        if stmt:
                            self.conn.execute(stmt)
                    timings[current_table] = time.time() - start

                # Extract table name (DROP TABLE IF EXISTS <name> [CASCADE];)
                parts = line.strip().split()
                table_name = parts[4] if len(parts) > 4 else "unknown"
                current_table = table_name.rstrip(";").replace("CASCADE", "").strip()
                current_sql = [line]
            elif line.strip().upper().startswith("CREATE INDEX"):
                current_sql.append(line)
            else:
                if current_table:
                    current_sql.append(line)

        # Don't forget the last table
        if current_table and current_sql:
            sql = "\n".join(current_sql)
            start = time.time()
            for stmt in sql.split(";"):
                stmt = stmt.strip()
                if stmt:
                    self.conn.execute(stmt)
            timings[current_table] = time.time() - start

        return timings

    def table_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}")
        return result.fetchone()[0]

    def load_csv(self, table_name: str, csv_path: str) -> float:
        """Load a CSV file into an existing table. Returns wall-clock time."""
        start = time.time()
        self.conn.execute(
            f"COPY {table_name} FROM '{csv_path}' (HEADER, DELIMITER ',', NULL '')"
        )
        elapsed = time.time() - start
        return elapsed
