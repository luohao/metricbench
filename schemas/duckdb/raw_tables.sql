-- Raw tables for experimentation benchmark (DuckDB)
-- These are the event-level tables that both approaches read from.

DROP TABLE IF EXISTS viewed_experiment CASCADE;
CREATE TABLE viewed_experiment (
  user_id       VARCHAR,
  anonymous_id  VARCHAR,
  session_id    VARCHAR,
  browser       VARCHAR,
  country       VARCHAR,
  timestamp     TIMESTAMP,
  experiment_id VARCHAR,
  variation_id  VARCHAR
);

DROP TABLE IF EXISTS orders CASCADE;
CREATE TABLE orders (
  user_id       VARCHAR,
  anonymous_id  VARCHAR,
  session_id    VARCHAR,
  browser       VARCHAR,
  country       VARCHAR,
  timestamp     TIMESTAMP,
  qty           INTEGER,
  amount        INTEGER
);

DROP TABLE IF EXISTS events CASCADE;
CREATE TABLE events (
  user_id       VARCHAR,
  anonymous_id  VARCHAR,
  session_id    VARCHAR,
  browser       VARCHAR,
  country       VARCHAR,
  timestamp     TIMESTAMP,
  event         VARCHAR,
  value         INTEGER
);

DROP TABLE IF EXISTS pages CASCADE;
CREATE TABLE pages (
  user_id       VARCHAR,
  anonymous_id  VARCHAR,
  session_id    VARCHAR,
  browser       VARCHAR,
  country       VARCHAR,
  timestamp     TIMESTAMP,
  path          VARCHAR
);

DROP TABLE IF EXISTS sessions CASCADE;
CREATE TABLE sessions (
  user_id       VARCHAR,
  anonymous_id  VARCHAR,
  session_id    VARCHAR,
  browser       VARCHAR,
  country       VARCHAR,
  sessionStart  TIMESTAMP,
  pages         INTEGER,
  duration      INTEGER
);

-- Load data from CSV files
COPY viewed_experiment FROM '/tmp/experimentation-benchmark/csv/exposures.csv' (HEADER, DELIMITER ',', NULL '');
COPY orders FROM '/tmp/experimentation-benchmark/csv/orders.csv' (HEADER, DELIMITER ',', NULL '');
COPY events FROM '/tmp/experimentation-benchmark/csv/events.csv' (HEADER, DELIMITER ',', NULL '');
COPY pages FROM '/tmp/experimentation-benchmark/csv/pages.csv' (HEADER, DELIMITER ',', NULL '');
COPY sessions FROM '/tmp/experimentation-benchmark/csv/sessions.csv' (HEADER, DELIMITER ',', NULL '');
