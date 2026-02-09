"""Postgres database engine for the benchmark."""

import time
import psycopg2
import psycopg2.extras


class PostgresEngine:
    """Manages Postgres connections and query execution."""

    def __init__(self, config: dict):
        self.config = config
        self.conn = None

    def connect(self):
        self.conn = psycopg2.connect(
            host=self.config.get("host", "localhost"),
            port=self.config.get("port", 5432),
            dbname=self.config["database"],
            user=self.config.get("user", "postgres"),
            password=self.config.get("password", ""),
        )
        self.conn.autocommit = True

    def close(self):
        if self.conn:
            self.conn.close()

    def execute_sql_file(self, filepath: str) -> float:
        """Execute a SQL file and return wall-clock time in seconds."""
        with open(filepath, "r") as f:
            sql = f.read()
        return self.execute(sql)

    def execute(self, sql: str) -> float:
        """Execute SQL and return wall-clock time in seconds."""
        start = time.time()
        with self.conn.cursor() as cur:
            cur.execute(sql)
        elapsed = time.time() - start
        return elapsed

    def execute_query(self, sql: str) -> dict:
        """Execute a query and return timing + results."""
        start = time.time()
        with self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Handle multi-statement SQL (e.g., CREATE TABLE + SELECT)
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            rows = []
            for stmt in statements:
                cur.execute(stmt)
                if cur.description:
                    rows = [dict(r) for r in cur.fetchall()]
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
                    with self.conn.cursor() as cur:
                        cur.execute(sql)
                    timings[current_table] = time.time() - start

                # Extract table name (DROP TABLE IF EXISTS <name> [CASCADE];)
                parts = line.strip().split()
                table_name = parts[4] if len(parts) > 4 else "unknown"
                current_table = table_name.rstrip(";").replace("CASCADE", "").strip()
                current_sql = [line]
            elif line.strip().upper().startswith("CREATE INDEX"):
                # Include index creation with the current table
                current_sql.append(line)
            else:
                if current_table:
                    current_sql.append(line)

        # Don't forget the last table
        if current_table and current_sql:
            sql = "\n".join(current_sql)
            start = time.time()
            with self.conn.cursor() as cur:
                cur.execute(sql)
            timings[current_table] = time.time() - start

        return timings

    def table_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        with self.conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            return cur.fetchone()[0]
