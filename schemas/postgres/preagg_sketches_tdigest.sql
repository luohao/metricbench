-- T-digest sketch tables (requires pg_tdigest extension)
-- Run this ONLY if you have pg_tdigest installed:
--   CREATE EXTENSION IF NOT EXISTS tdigest;
--
-- T-digests are compact, mergeable sketches for approximate quantile computation.
-- Each sketch is ~1KB regardless of input size, and can be merged across days.

CREATE EXTENSION IF NOT EXISTS tdigest;

DROP TABLE IF EXISTS shared_sketches_tdigest CASCADE;
CREATE TABLE shared_sketches_tdigest AS
SELECT
  user_id,
  CAST(timestamp AS DATE) AS metric_date,
  tdigest(amount, 100) AS amount_digest,
  tdigest(amount, 100)
    FILTER (WHERE amount != 0) AS amount_digest_nonzero
FROM orders
WHERE amount IS NOT NULL
GROUP BY user_id, CAST(timestamp AS DATE);

CREATE INDEX idx_shared_tdigest_user_date ON shared_sketches_tdigest (user_id, metric_date);
