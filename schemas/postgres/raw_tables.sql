-- Raw tables for experimentation benchmark
-- These are the event-level tables that both approaches read from.

DROP TABLE IF EXISTS viewed_experiment CASCADE;
CREATE TABLE viewed_experiment (
  user_id       VARCHAR(16),
  anonymous_id  VARCHAR(32),
  session_id    VARCHAR(32),
  browser       VARCHAR(20),
  country       VARCHAR(2),
  timestamp     TIMESTAMP,
  experiment_id VARCHAR(32),
  variation_id  VARCHAR(32)
);

DROP TABLE IF EXISTS orders CASCADE;
CREATE TABLE orders (
  user_id       VARCHAR(16),
  anonymous_id  VARCHAR(32),
  session_id    VARCHAR(32),
  browser       VARCHAR(20),
  country       VARCHAR(2),
  timestamp     TIMESTAMP,
  qty           INTEGER,
  amount        INTEGER
);

DROP TABLE IF EXISTS events CASCADE;
CREATE TABLE events (
  user_id       VARCHAR(16),
  anonymous_id  VARCHAR(32),
  session_id    VARCHAR(32),
  browser       VARCHAR(20),
  country       VARCHAR(2),
  timestamp     TIMESTAMP,
  event         VARCHAR(32),
  value         INTEGER
);

DROP TABLE IF EXISTS pages CASCADE;
CREATE TABLE pages (
  user_id       VARCHAR(16),
  anonymous_id  VARCHAR(32),
  session_id    VARCHAR(32),
  browser       VARCHAR(20),
  country       VARCHAR(2),
  timestamp     TIMESTAMP,
  path          VARCHAR(32)
);

DROP TABLE IF EXISTS sessions CASCADE;
CREATE TABLE sessions (
  user_id       VARCHAR(16),
  anonymous_id  VARCHAR(32),
  session_id    VARCHAR(32),
  browser       VARCHAR(20),
  country       VARCHAR(2),
  sessionStart  TIMESTAMP,
  pages         INTEGER,
  duration      INTEGER
);

-- Load data from CSV files
-- Adjust paths as needed for your environment
\copy viewed_experiment FROM '/tmp/experimentation-benchmark/csv/exposures.csv' WITH DELIMITER ',' CSV HEADER NULL AS '';
\copy orders FROM '/tmp/experimentation-benchmark/csv/orders.csv' WITH DELIMITER ',' CSV HEADER NULL AS '';
\copy events FROM '/tmp/experimentation-benchmark/csv/events.csv' WITH DELIMITER ',' CSV HEADER NULL AS '';
\copy pages FROM '/tmp/experimentation-benchmark/csv/pages.csv' WITH DELIMITER ',' CSV HEADER NULL AS '';
\copy sessions FROM '/tmp/experimentation-benchmark/csv/sessions.csv' WITH DELIMITER ',' CSV HEADER NULL AS '';

-- Indexes for on-demand approach (simulate realistic indexing)
CREATE INDEX idx_exposure_expid ON viewed_experiment (experiment_id, timestamp);
CREATE INDEX idx_orders_user ON orders (user_id, timestamp);
CREATE INDEX idx_events_user ON events (user_id, timestamp);
CREATE INDEX idx_events_event ON events (event, user_id, timestamp);
