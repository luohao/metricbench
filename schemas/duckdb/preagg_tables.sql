-- Pre-aggregated shared tables (Facebook-style approach) -- DuckDB
-- These are built ONCE from the raw tables and reused across all experiments.
-- The cost of building these tables is the "pipeline cost" in the benchmark.

-- ============================================================================
-- Table 1: Shared Exposures
-- One row per (user, experiment, variation, date)
-- Serves ALL experiments without re-scanning raw exposure events.
-- ============================================================================

DROP TABLE IF EXISTS shared_exposures CASCADE;
CREATE TABLE shared_exposures AS
SELECT
  user_id,
  anonymous_id,
  experiment_id,
  variation_id,
  CAST(timestamp AS DATE) AS exposure_date,
  MIN(timestamp) AS first_exposure_timestamp,
  -- Carry dimension columns from exposure table
  MIN(browser) AS browser,
  MIN(country) AS country
FROM viewed_experiment
GROUP BY user_id, anonymous_id, experiment_id, variation_id, CAST(timestamp AS DATE);

-- ============================================================================
-- Table 2: Shared Daily User Metrics
-- One row per (user, date) with ALL metric columns pre-aggregated.
-- Serves ALL metrics without re-scanning raw event tables.
-- ============================================================================

DROP TABLE IF EXISTS shared_metrics_daily CASCADE;
CREATE TABLE shared_metrics_daily AS
SELECT
  user_id,
  metric_date,

  -- Binomial: did user have an add-to-cart event today?
  MAX(CASE WHEN source = 'events' AND event = 'Add to Cart' THEN 1 ELSE 0 END) AS has_add_to_cart,

  -- Binomial: did user have a Cart Loaded event today? (for activation metric)
  MAX(CASE WHEN source = 'events' AND event = 'Cart Loaded' THEN 1 ELSE 0 END) AS has_cart_loaded,

  -- Binomial: did user make any purchase today?
  MAX(CASE WHEN source = 'orders' THEN 1 ELSE 0 END) AS has_purchase,

  -- Binomial: did user make a large purchase (amount >= 10) today?
  MAX(CASE WHEN source = 'orders' AND amount >= 10 THEN 1 ELSE 0 END) AS has_large_purchase,

  -- Count: total items purchased today
  COALESCE(SUM(CASE WHEN source = 'orders' THEN qty ELSE 0 END), 0) AS purchased_items,

  -- Count: number of orders today
  COUNT(CASE WHEN source = 'orders' THEN 1 END) AS purchase_count,

  -- Count: total revenue today (preserves NULL for null amounts)
  SUM(CASE WHEN source = 'orders' THEN amount END) AS purchased_value,

  -- Activity flag (for COUNT DISTINCT date metric)
  1 AS has_activity

FROM (
  -- Combine orders and events into a unified stream
  SELECT
    user_id,
    CAST(timestamp AS DATE) AS metric_date,
    'orders' AS source,
    qty,
    amount,
    NULL AS event
  FROM orders

  UNION ALL

  SELECT
    user_id,
    CAST(timestamp AS DATE) AS metric_date,
    'events' AS source,
    NULL AS qty,
    NULL AS amount,
    event
  FROM events
) combined
GROUP BY user_id, metric_date;

-- ============================================================================
-- Table 3: Shared Activation Events
-- One row per (user, date) for activation metric lookups.
-- Pre-aggregated separately for efficient JOIN.
-- ============================================================================

DROP TABLE IF EXISTS shared_activations CASCADE;
CREATE TABLE shared_activations AS
SELECT
  user_id,
  CAST(timestamp AS DATE) AS activation_date,
  MIN(timestamp) AS first_activation_timestamp
FROM events
WHERE event = 'Cart Loaded'
GROUP BY user_id, CAST(timestamp AS DATE);

-- ============================================================================
-- Table 4a: Shared Sketches (Array-based)
-- Preserves event-level distribution for quantile metrics.
-- ============================================================================

DROP TABLE IF EXISTS shared_sketches_array CASCADE;
CREATE TABLE shared_sketches_array AS
SELECT
  user_id,
  CAST(timestamp AS DATE) AS metric_date,
  ARRAY_AGG(amount ORDER BY amount)
    FILTER (WHERE amount IS NOT NULL) AS amount_values,
  ARRAY_AGG(amount ORDER BY amount)
    FILTER (WHERE amount IS NOT NULL AND amount != 0) AS amount_values_nonzero
FROM orders
GROUP BY user_id, CAST(timestamp AS DATE);
