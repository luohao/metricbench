# Benchmark Walkthrough: Input Data, Metrics, Experiments, and Pre-Aggregation

This document explains everything in the benchmark: what data goes in, what experiments and metrics are defined, how the two SQL approaches work, and how pre-aggregation changes the query.

---

## Table of Contents

- [Part 1: Input Data](#part-1-input-data)
- [Part 2: What Is an Experiment?](#part-2-what-is-an-experiment)
- [Part 3: What Is a Metric?](#part-3-what-is-a-metric)
- [Part 4: The 22 Experiment Configurations](#part-4-the-22-experiment-configurations)
- [Part 5: The 33 Metric Definitions](#part-5-the-33-metric-definitions)
- [Part 6: On-Demand Approach (How Most Platforms Work)](#part-6-on-demand-approach)
- [Part 7: Pre-Aggregation Approach (How FAANG Works)](#part-7-pre-aggregation-approach)
- [Part 8: CUPED (Regression Adjustment) in Both Approaches](#part-8-cuped)
- [Part 9: Quantile Metrics and Data Sketches](#part-9-quantile-metrics-and-data-sketches)
- [Part 10: The Partial-Day Problem and Weighting](#part-10-the-partial-day-problem)

---

## Part 1: Input Data

The benchmark simulates an e-commerce platform running an A/B test on its checkout page layout. A data generator (`data/generate_data.py`) produces synthetic user behavior across 5 raw event tables:

### The 5 Raw Tables

**`viewed_experiment`** -- Who saw which variation of the A/B test, and when:

```
user_id | anonymous_id | session_id | browser | country | timestamp           | experiment_id   | variation_id
--------|-------------|-----------|---------|---------|---------------------|-----------------|-------------
u000042 | a1b2c3d4...  | u000042_s5 | Chrome  | US      | 2021-12-15 09:23:00 | checkout-layout | 1
u000042 | a1b2c3d4...  | u000042_s8 | Chrome  | US      | 2021-12-20 14:11:00 | checkout-layout | 1
u000099 | e5f6g7h8...  | u000099_s3 | Safari  | UK      | 2021-12-15 18:45:00 | checkout-layout | 0
```

Each row = one time a user was shown the experiment. Users can have multiple exposure rows (re-exposures on different days).

**`orders`** -- Purchase events:

```
user_id | anonymous_id | session_id | browser | country | timestamp           | qty | amount
--------|-------------|-----------|---------|---------|---------------------|-----|-------
u000042 | a1b2c3d4...  | u000042_s5 | Chrome  | US      | 2021-12-15 09:35:00 | 2   | 20
u000042 | a1b2c3d4...  | u000042_s8 | Chrome  | US      | 2021-12-20 14:30:00 | 1   | NULL
u000099 | e5f6g7h8...  | u000099_s3 | Safari  | UK      | 2021-12-15 19:00:00 | 3   | 50
```

`amount` can be NULL (~10% of orders). `qty` is always present.

**`events`** -- Behavioral events (Add to Cart, Cart Loaded, Search, Wishlist):

```
user_id | anonymous_id | session_id | browser | country | timestamp           | event        | value
--------|-------------|-----------|---------|---------|---------------------|-------------|------
u000042 | a1b2c3d4...  | u000042_s5 | Chrome  | US      | 2021-12-15 09:25:00 | Add to Cart  | 3
u000042 | a1b2c3d4...  | u000042_s5 | Chrome  | US      | 2021-12-15 09:26:00 | Cart Loaded  | 1
u000099 | e5f6g7h8...  | u000099_s3 | Safari  | UK      | 2021-12-15 18:50:00 | Search       | 1
```

**`pages`** -- Page views:

```
user_id | anonymous_id | session_id | browser | country | timestamp           | path
--------|-------------|-----------|---------|---------|---------------------|----------
u000042 | a1b2c3d4...  | u000042_s5 | Chrome  | US      | 2021-12-15 09:20:00 | /products
u000042 | a1b2c3d4...  | u000042_s5 | Chrome  | US      | 2021-12-15 09:22:00 | /cart
```

**`sessions`** -- Session summaries:

```
user_id | anonymous_id | session_id | browser | country | sessionStart        | pages | duration
--------|-------------|-----------|---------|---------|---------------------|-------|--------
u000042 | a1b2c3d4...  | u000042_s5 | Chrome  | US      | 2021-12-15 09:20:00 | 4     | 300
```

### Data Scale

By default, 5,000 users over 120 days. The generator is configurable:

| Scale | Users | ~Exposures | ~Orders | ~Events |
|---|---|---|---|---|
| Small | 1,000 | 5K | 15K | 40K |
| Default | 5,000 | 25K | 75K | 200K |
| Large | 100,000 | 500K | 1.5M | 4M |
| Extreme | 1,000,000 | 5M | 15M | 40M |

### Data Distributions

- **Browsers**: Chrome 65%, Safari 20%, Firefox 15%
- **Countries**: US 50%, UK 20%, CA 15%, AU 15%
- **Variations**: 34% control (0), 33% variation 1, 33% variation 2
- **Order amounts**: $1-$100 with right-skew (most orders $2-$20, few at $50-$100)
- **NULL amounts**: ~10% of orders have NULL amount

---

## Part 2: What Is an Experiment?

An A/B test experiment has:

1. **Exposure**: Users are shown one of several variations (Control, Variation 1, Variation 2)
2. **Time window**: The experiment runs from a start date to an end date
3. **Metric measurement**: For each user, compute a metric value within their conversion window
4. **Statistical test**: Compare metric values between variations to determine if the treatment had an effect

The SQL's job is step 3: produce aggregated statistics per variation that the stats engine can use for step 4.

### The Core SQL Pattern

Every experiment analysis query follows this pattern:

```
Step 1: Build a UNITS table (one row per user with their variation + first exposure time)
Step 2: Build a METRIC table (events from the metric source, filtered by conversion window)
Step 3: JOIN units with metrics, aggregate per user
Step 4: GROUP BY variation, return SUM, SUM_SQUARES, COUNT for the stats engine
```

The stats engine receives something like:

```
variation | users | main_sum | main_sum_squares
----------|-------|----------|------------------
0         | 1700  | 510      | 3200
1         | 1650  | 495      | 3100
2         | 1680  | 520      | 3300
```

From `main_sum` and `main_sum_squares`, it can compute mean, variance, confidence intervals, and p-values.

---

## Part 3: What Is a Metric?

A metric defines HOW to compute a value for each user. There are 5 types:

### Binomial (0 or 1 per user)

"Did the user do X within their conversion window?"

```sql
-- Example: Did user add anything to cart?
MAX(CASE WHEN event = 'Add to Cart' AND timestamp WITHIN window THEN 1 ELSE 0 END) AS value
```

Result per user: 0 or 1. Stats engine computes conversion rate.

### Count (numeric sum per user)

"How much of X did the user do within their window?"

```sql
-- Example: Total items purchased
SUM(CASE WHEN timestamp WITHIN window THEN qty ELSE 0 END) AS value
```

Result per user: 0, 1, 2, 5, etc. Stats engine computes mean and variance.

### Ratio (numerator / denominator)

"What is the ratio of X to Y per user?"

```sql
-- Example: Revenue per item purchased
-- numerator: SUM(amount) per user
-- denominator: SUM(qty) per user
-- Stats engine computes the ratio using the delta method for CIs
```

The SQL returns both numerator and denominator sums separately. The stats engine combines them.

### Quantile (Nth percentile across events or users)

"What is the 90th percentile of order amounts?"

```sql
-- Example: P90 of order amounts
PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount)
```

Two sub-types:
- **Event-level**: P90 across all individual order amounts (each order is a data point)
- **Unit-level**: P90 across per-user total amounts (each user's sum is a data point)

### Special Features That Modify Any Metric

**CUPED (regression adjustment):** Uses 4 days of pre-exposure data as a covariate to reduce variance. Returns additional columns: `covariate_sum`, `covariate_sum_squares`, `main_covariate_sum_product`.

**Percentile capping:** After computing per-user totals, find the 90th percentile across users and cap any user's value at that threshold. Reduces impact of outliers.

**Custom aggregation:** Override the default `SUM(value)` with `COUNT(*)` or `COUNT(DISTINCT value)`.

---

## Part 4: The 22 Experiment Configurations

All 22 experiments use the same data and the same `checkout-layout` experiment. They differ in HOW the analysis SQL is built. Each one tests a different combination of 8 features:

### Feature 1: Attribution Model

How to determine a user's conversion window.

**First Exposure** (default): Window starts at the user's FIRST exposure timestamp.

```
User exposed Dec 15 at 9am, again Dec 20.
Window: Dec 15 09:00 → Dec 18 09:00 (72 hours from FIRST exposure)
```

**Experiment Duration**: Window extends to the experiment end date for everyone.

```
User exposed Dec 15 at 9am.
Window: Dec 15 09:00 → Feb 1 00:00 (experiment end, same for all users)
```

### Feature 2: Activation Metric

Only analyze users who triggered a specific event ("Cart Loaded") within their window.

```
Without activation: 5000 users analyzed (all exposed users)
With activation:    4500 users analyzed (only those who loaded the cart)
```

Users who never activated are excluded from BOTH numerator and denominator. This gives a cleaner causal estimate on users who actually experienced the treatment.

### Feature 3: Conversion Window Delay

Shift the start of the conversion window forward or backward.

**Negative delay (-24h):** Count events starting 24 hours BEFORE exposure.

```
Timeline: [-24h] ======= [exposure] ======= [+24h]
                   ^ window starts here              ^ window ends here (48h total)
```

Used for: Pre-period analysis, detecting pre-existing trends, CUPED covariates.

**Positive delay (+24h):** Skip the first 24 hours after exposure.

```
Timeline: [exposure] --- skip 24h --- [+24h] ======= [+72h]
                                         ^ window starts    ^ window ends (48h)
```

Used for: Delayed effects (email campaigns, habit formation).

### Feature 4: Lookback Window

Instead of counting forward from exposure, count events in the last N hours of the experiment.

```
Timeline:                              [Experiment End: Feb 1]
                                              |
                Jan 30 00:00 ================|
                   ^ lookback start (48h)    ^ end
```

Used for: Measuring steady-state behavior after users have adapted.

### Feature 5: Skip Partial Data

Drop users whose conversion window extends past the experiment end date.

```
User exposed Jan 31, window = 72h → extends to Feb 3 (past experiment end Feb 1)
With skipPartialData: EXCLUDED (incomplete window)
Without: INCLUDED (only 1 day of data vs 3 for others)
```

### Feature 6: Dimensions

Break down results by a categorical variable.

- **Date dimension**: GROUP BY date of first exposure
- **Experiment field dimension**: GROUP BY browser (from exposure table)
- **User attribute dimension**: GROUP BY country (from separate table)
- **Activation dimension**: GROUP BY "Activated" vs "Not Activated"

### Feature 7: Segments

Filter to a subpopulation before analysis.

```sql
-- Chrome-only segment
WHERE browser = 'Chrome'
```

Only Chrome users are included in the analysis. INNER JOIN (not LEFT JOIN) ensures non-matching users are excluded.

### Feature 8: Identity Joins

When the activation metric or metric source uses a different ID type (e.g., `anonymous_id` instead of `user_id`), an identity join resolves the mapping:

```sql
SELECT DISTINCT user_id, anonymous_id FROM orders
```

### The 22 Configs as Feature Combinations

| # | Config | Attribution | Activation | Delay | Lookback | SkipPartial | Dimension | Segment |
|---|---|---|---|---|---|---|---|---|
| 1 | base | first | - | 0 | - | - | - | - |
| 2 | skipPartialData | first | - | 0 | - | yes | - | - |
| 3 | base_allExposures | duration | - | 0 | - | - | - | - |
| 4 | activation | first | Cart Loaded | 0 | - | - | - | - |
| 5 | activation_anonymous | first | Cart Loaded (anon) | 0 | - | - | - | - |
| 6 | activation_allExposures | duration | Cart Loaded | 0 | - | - | - | - |
| 7 | negativeconversion | first | - | -24h | - | - | - | - |
| 8 | base_allExposures_negativeconversion | duration | - | -24h | - | - | - | - |
| 9 | activation_negativeconversion | first | Cart Loaded | -24h | - | - | - | - |
| 10 | activation_allExposures_negativeconversion | duration | Cart Loaded | -24h | - | - | - | - |
| 11 | positiveconversion | first | - | +24h | - | - | - | - |
| 12 | lookback | first | - | +24h | 48h | - | - | - |
| 13 | lookback_allExposures | duration | - | +24h | 48h | - | - | - |
| 14 | lookback_skipPartial | duration | - | +24h | 48h | yes | - | - |
| 15 | base_allExposures_positiveconversion | duration | - | +24h | - | - | - | - |
| 16 | activation_positiveconversion | first | Cart Loaded | +24h | - | - | - | - |
| 17 | activation_allExposures_positiveconversion | duration | Cart Loaded | +24h | - | - | - | - |
| 18 | dimension_date | first | - | 0 | - | - | date | - |
| 19 | dimension_experiment | first | - | 0 | - | - | browser | - |
| 20 | dimension_user | first | - | 0 | - | - | country | - |
| 21 | dimension_activation | first | Cart Loaded | 0 | - | - | activation | - |
| 22 | filter_segment | first | - | 0 | - | - | - | Chrome |

---

## Part 5: The 33 Metric Definitions

### Binomial Metrics (4)

| # | Metric | Source | What It Measures | Special |
|---|---|---|---|---|
| 1 | any_item_in_cart | events WHERE 'Add to Cart' | Did user add to cart? | - |
| 2 | any_item_in_cart_cuped | events WHERE 'Add to Cart' | Did user add to cart? | CUPED 4-day |
| 3 | fact_any_item_in_cart | events WHERE 'Add to Cart' | Same, fact table variant | - |
| 4 | fact_any_item_in_cart_cuped | events WHERE 'Add to Cart' | Same, fact table variant | CUPED 4-day |

### Count Metrics (15)

| # | Metric | Source | Value Column | Aggregation | Special |
|---|---|---|---|---|---|
| 5 | purchased_items | orders | qty | SUM | - |
| 6 | purchased_items_anonymous | orders | qty | SUM | anonymous_id + identity join |
| 7 | purchased_items_custom_agg | orders | qty | COUNT(*) | Custom aggregation |
| 8 | purchased_items_custom_agg_cuped | orders | qty | COUNT(*) | Custom agg + CUPED 4-day |
| 9 | purchased_value | orders | amount | SUM | Nullable values |
| 10 | purchased_value_pctilecap | orders | amount | SUM | Percentile cap P90 |
| 11 | purchased_value_pctilecap_ignorezeros | orders | amount | SUM | Percentile cap P90, ignore zeros |
| 12 | count_distinct_date | orders | DATE(timestamp) | COUNT(DISTINCT) | Custom value + aggregation |
| 13 | purchased_items_cuped | orders | qty | SUM | CUPED 4-day |
| 14 | purchased_value_cuped | orders | amount | SUM | CUPED 4-day |
| 15 | fact_purchased_items | orders | qty | SUM | Fact table variant |
| 16 | fact_purchased_items_cuped | orders | qty | SUM | Fact + CUPED 4-day |
| 17 | fact_purchased_value | orders | amount | SUM | Fact table variant |
| 18 | fact_purchased_value_cuped | orders | amount | SUM | Fact + CUPED 4-day |
| 19 | fact_purchased_value_pctilecap | orders | amount | SUM | Fact + percentile cap P90 |
| 20 | fact_purchased_value_pctilecap_ignorezeros | orders | amount | SUM | Fact + pctile cap, ignore zeros |

### Ratio Metrics (9)

| # | Metric | Numerator | Denominator | Special |
|---|---|---|---|---|
| 21 | ratio_purchase_over_cart | has_purchase (binomial) | has_add_to_cart (binomial) | - |
| 22 | ratio_revenue_over_cart | SUM(amount) | has_add_to_cart (binomial) | - |
| 23 | ratio_revenue_over_items | SUM(amount) | SUM(qty) | - |
| 24 | ratio_large_purchase_over_purchase_over_cart | has_large_purchase | has_purchase | Chained ratio |
| 25 | ratio_revenue_over_revenue_pctilecap | SUM(amount) | SUM(amount) capped P90 | Denominator capped |
| 26 | ratio_revenue_over_items_custom | COUNT(*) orders | COUNT(*) orders | Custom agg |
| 27 | fact_ratio_purchase_over_cart | has_purchase | has_add_to_cart | Fact variant |
| 28 | fact_ratio_revenue_over_cart | SUM(amount) | has_add_to_cart | Fact variant |
| 29 | fact_ratio_revenue_over_items | SUM(amount) | SUM(qty) | Fact variant |

### Quantile Metrics (4)

| # | Metric | Value | Quantile | Type | Ignore Zeros |
|---|---|---|---|---|---|
| 30 | quantile_purchased_value_event_p90 | amount | 0.9 | event-level | no |
| 31 | quantile_purchased_value_unit_p90 | amount | 0.9 | unit-level | no |
| 32 | quantile_purchased_value_event_p90_ignorezeros | amount | 0.9 | event-level | yes |
| 33 | quantile_purchased_value_unit_p90_ignorezeros | amount | 0.9 | unit-level | yes |

---

## Part 6: On-Demand Approach

This is how most experimentation platforms (including GrowthBook) work. For each experiment, the SQL scans the raw event tables from scratch.

### Example: `base` experiment + `purchased_items` metric

```sql
-- STEP 1: Read raw exposure events
WITH __rawExperiment AS (
  SELECT user_id, timestamp, variation_id, browser, country
  FROM viewed_experiment                          -- Full table scan
  WHERE experiment_id = 'checkout-layout'
    AND timestamp >= '2021-11-03'
    AND timestamp <= '2022-02-01'
),

-- STEP 2: Deduplicate to one row per user
__units AS (
  SELECT
    user_id,
    CASE WHEN COUNT(DISTINCT variation_id) > 1 THEN '__multiple__'
         ELSE MAX(variation_id) END AS variation,
    MIN(timestamp) AS first_exposure              -- First exposure attribution
  FROM __rawExperiment
  GROUP BY user_id
),

-- STEP 3: Read raw metric events
__metric AS (
  SELECT user_id, timestamp, qty AS value
  FROM orders                                     -- Full table scan
),

-- STEP 4: Join and apply conversion window
__userMetric AS (
  SELECT
    u.user_id,
    u.variation,
    COALESCE(SUM(
      CASE
        WHEN m.timestamp >= u.first_exposure              -- after exposure
         AND m.timestamp <= u.first_exposure + INTERVAL '72 hours'  -- within 72h window
        THEN m.value
        ELSE NULL
      END
    ), 0) AS value
  FROM __units u
  LEFT JOIN __metric m ON m.user_id = u.user_id
  WHERE u.variation != '__multiple__'
  GROUP BY u.user_id, u.variation
)

-- STEP 5: Aggregate per variation
SELECT
  variation,
  COUNT(*) AS users,
  SUM(value) AS main_sum,
  SUM(POWER(value, 2)) AS main_sum_squares
FROM __userMetric
GROUP BY variation;
```

**Cost:** Two full table scans (viewed_experiment + orders) per experiment per metric.
If you have 50 experiments x 10 metrics = 500 full scans of the orders table.

---

## Part 7: Pre-Aggregation Approach

This is how FAANG-scale systems work. Raw tables are scanned ONCE to build shared tables. All subsequent experiment analyses are lightweight joins.

### The 4 Shared Tables

**Table 1: `shared_exposures`** -- built from `viewed_experiment`:

```
user_id | experiment_id   | variation_id | exposure_date | first_exposure_timestamp | browser | country
--------|-----------------|-------------|---------------|------------------------|---------|--------
u000042 | checkout-layout | 1           | 2021-12-15    | 2021-12-15 09:23:00    | Chrome  | US
u000099 | checkout-layout | 0           | 2021-12-15    | 2021-12-15 18:45:00    | Safari  | UK
```

One row per (user, experiment, variation, date). Indexed on `(experiment_id, exposure_date)`.

**Table 2: `shared_metrics_daily`** -- built from `orders` + `events`:

```
user_id | metric_date | has_add_to_cart | has_purchase | purchased_items | purchase_count | purchased_value | has_activity
--------|------------|----------------|-------------|----------------|---------------|----------------|------------
u000042 | 2021-12-15 | 1              | 1           | 2              | 1             | 20             | 1
u000042 | 2021-12-20 | 0              | 1           | 1              | 1             | NULL           | 1
u000099 | 2021-12-15 | 0              | 1           | 3              | 1             | 50             | 1
```

One row per (user, date) with ALL metric columns pre-computed. Every metric the benchmark uses is a column in this table.

**Table 3: `shared_activations`** -- built from `events WHERE event = 'Cart Loaded'`:

```
user_id | activation_date | first_activation_timestamp
--------|----------------|---------------------------
u000042 | 2021-12-15     | 2021-12-15 09:26:00
```

**Table 4: `shared_sketches_array`** -- built from `orders`, preserves event-level distribution:

```
user_id | metric_date | amount_values     | amount_values_nonzero
--------|------------|-------------------|---------------------
u000042 | 2021-12-15 | {20}              | {20}
u000042 | 2021-12-20 | {NULL}            | {}
u000099 | 2021-12-15 | {50}              | {50}
```

### Example: Same query using pre-aggregated tables

```sql
-- STEP 1: Get units from shared table (no raw table scan!)
WITH __units AS (
  SELECT
    user_id,
    CASE WHEN COUNT(DISTINCT variation_id) > 1 THEN '__multiple__'
         ELSE MAX(variation_id) END AS variation,
    MIN(first_exposure_timestamp) AS first_exposure
  FROM shared_exposures                           -- Pre-aggregated, indexed
  WHERE experiment_id = 'checkout-layout'
    AND exposure_date >= '2021-11-03'
    AND exposure_date <= '2022-02-01'
  GROUP BY user_id
),

-- STEP 2: Join with shared daily metrics (no raw table scan!)
__userMetric AS (
  SELECT
    u.user_id,
    u.variation,
    COALESCE(SUM(m.purchased_items), 0) AS value  -- Pre-aggregated column
  FROM __units u
  LEFT JOIN shared_metrics_daily m                 -- Pre-aggregated, indexed
    ON m.user_id = u.user_id
    AND m.metric_date >= CAST(u.first_exposure AS DATE)       -- Date-level window
    AND m.metric_date <= CAST(u.first_exposure + INTERVAL '3 days' AS DATE)
  WHERE u.variation != '__multiple__'
  GROUP BY u.user_id, u.variation
)

-- STEP 3: Aggregate per variation (same output as on-demand)
SELECT
  variation,
  COUNT(*) AS users,
  SUM(value) AS main_sum,
  SUM(POWER(value, 2)) AS main_sum_squares
FROM __userMetric
GROUP BY variation;
```

**Key differences from on-demand:**
- `FROM shared_exposures` instead of `FROM viewed_experiment` (pre-filtered, indexed)
- `FROM shared_metrics_daily` instead of `FROM orders` (pre-aggregated, one row per user per day vs one row per event)
- `m.purchased_items` instead of computing `SUM(CASE WHEN ...)` -- the aggregation is pre-done
- Date-level window (`metric_date >= ...`) instead of timestamp-level (`timestamp >= ...`)

**Cost:** Two lightweight joins (shared_exposures + shared_metrics_daily). Both tables are smaller than raw tables and indexed. The pipeline cost (building 4 shared tables) is paid once and amortized across all experiments.

### How Each Experiment Feature Works in Pre-Agg

| Feature | On-Demand SQL Change | Pre-Agg SQL Change |
|---|---|---|
| Attribution (duration) | `m.timestamp <= '2022-02-01'` | `m.metric_date <= '2022-02-01'` |
| Activation | CTE + LEFT JOIN + filter in WHERE | JOIN shared_activations + filter |
| Negative delay (-24h) | `m.timestamp >= exposure - 24h` | `m.metric_date >= DATE(exposure - 1 day)` |
| Positive delay (+24h) | `m.timestamp >= exposure + 24h` | `m.metric_date >= DATE(exposure + 1 day)` |
| Lookback (48h) | `m.timestamp + 48h >= end_date` | `m.metric_date + 2 >= end_date` |
| Skip partial | `first_exposure <= end_date` in WHERE | Same |
| Dimensions | Additional GROUP BY column | Same |
| Segments | INNER JOIN segment CTE | Same |

---

## Part 8: CUPED

CUPED (Controlled-experiment Using Pre-Experiment Data) uses pre-exposure metric values as a covariate to reduce variance and detect effects faster.

### How It Works

For each user, compute TWO values:
1. **Current period**: Metric value within the conversion window (after exposure)
2. **Pre-period**: Same metric value in the 4 days BEFORE exposure (covariate)

The stats engine uses the correlation between pre-period and current-period to reduce noise.

### On-Demand CUPED SQL

```sql
-- Current period (same as standard metric)
__userMetric AS (
  SELECT u.user_id, u.variation,
    SUM(CASE WHEN m.timestamp >= u.first_exposure
              AND m.timestamp <= u.first_exposure + INTERVAL '72 hours'
             THEN m.value END) AS value
  FROM __units u LEFT JOIN __metric m ON m.user_id = u.user_id
  GROUP BY u.user_id, u.variation
),

-- Pre-period (4 days before exposure)
__userCovariate AS (
  SELECT u.user_id,
    SUM(CASE WHEN m.timestamp >= u.first_exposure - INTERVAL '4 days'
              AND m.timestamp < u.first_exposure
             THEN m.value END) AS covariate_value
  FROM __units u LEFT JOIN __metric m ON m.user_id = u.user_id
  GROUP BY u.user_id
)

-- Return both current and pre-period values
SELECT
  um.variation,
  COUNT(*) AS users,
  SUM(um.value) AS main_sum,
  SUM(POWER(um.value, 2)) AS main_sum_squares,
  SUM(uc.covariate_value) AS covariate_sum,
  SUM(POWER(uc.covariate_value, 2)) AS covariate_sum_squares,
  SUM(um.value * uc.covariate_value) AS main_covariate_sum_product
FROM __userMetric um
LEFT JOIN __userCovariate uc ON uc.user_id = um.user_id
GROUP BY um.variation;
```

### Pre-Agg CUPED SQL

```sql
-- Current period from daily aggregates
__userMetric AS (
  SELECT u.user_id, u.variation,
    COALESCE(SUM(m.purchased_items), 0) AS value
  FROM __units u
  LEFT JOIN shared_metrics_daily m ON m.user_id = u.user_id
    AND m.metric_date >= CAST(u.first_exposure AS DATE)
    AND m.metric_date <= CAST(u.first_exposure + INTERVAL '3 days' AS DATE)
  GROUP BY u.user_id, u.variation
),

-- Pre-period from daily aggregates (4 days before exposure)
__userCovariate AS (
  SELECT u.user_id,
    COALESCE(SUM(m.purchased_items), 0) AS covariate_value
  FROM __units u
  LEFT JOIN shared_metrics_daily m ON m.user_id = u.user_id
    AND m.metric_date >= CAST(u.first_exposure - INTERVAL '4 days' AS DATE)
    AND m.metric_date < CAST(u.first_exposure AS DATE)
  GROUP BY u.user_id
)
-- Same SELECT output as on-demand
```

The pre-agg version is simpler: no CASE WHEN timestamp logic, just date range filters on the pre-aggregated table.

---

## Part 9: Quantile Metrics and Data Sketches

Quantile metrics (P90 of order amounts) need individual event values -- daily SUM destroys this information. Two solutions:

### Array Approach (Postgres Native)

Store individual values as arrays in the daily table:

```sql
-- shared_sketches_array has:
--   amount_values = {5, 10, 20, 100}  (all amounts that day)
--   amount_values_nonzero = {5, 10, 20, 100}  (excluding zeros)

-- Analysis: unnest arrays across days, compute percentile
WITH __allEvents AS (
  SELECT u.variation, UNNEST(s.amount_values) AS event_value
  FROM __units u
  JOIN shared_sketches_array s ON s.user_id = u.user_id
    AND s.metric_date >= CAST(u.first_exposure AS DATE)
    AND s.metric_date <= CAST(u.first_exposure + INTERVAL '3 days' AS DATE)
)
SELECT variation,
  PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY event_value) AS p90
FROM __allEvents
GROUP BY variation;
```

**Trade-off:** Exact results, but arrays can be large and UNNEST is expensive at scale.

### T-Digest Approach (Extension Required)

T-digests are compact sketches (~1KB) that can be merged and queried for approximate quantiles:

```sql
-- shared_sketches_tdigest has:
--   amount_digest = tdigest(amount, 100)

-- Analysis: merge daily digests, extract quantile
SELECT variation,
  tdigest_percentile(tdigest_union_agg(s.amount_digest), 0.9) AS p90
FROM __units u
JOIN shared_sketches_tdigest s ON s.user_id = u.user_id AND ...
GROUP BY variation;
```

**Trade-off:** Approximate (~1% error), but compact and fast regardless of data volume.

---

## Part 10: The Partial-Day Problem

The pre-agg approach uses daily granularity. A 72-hour conversion window becomes "3 calendar days." This creates a systematic bias:

### The Problem

User exposed at Dec 15, 6 PM:
- **On-demand** (timestamp precision): Counts events from Dec 15 18:00 to Dec 18 18:00 (exactly 72 hours)
- **Pre-agg** (daily granularity): Counts events from Dec 15 to Dec 18 (includes 18 hours of pre-exposure events on Dec 15)

The first day is a "partial day" -- only 6 hours of the 24-hour daily aggregate are actually post-exposure.

### The Weighted Solution

Apply a fractional weight to the first day based on when the user was exposed:

```sql
SUM(
  m.purchased_items *
  CASE
    WHEN m.metric_date = CAST(u.first_exposure AS DATE)
    THEN (24.0 - EXTRACT(HOUR FROM u.first_exposure)) / 24.0  -- e.g., 6 PM → weight 0.25
    ELSE 1.0
  END
) AS value
```

User exposed at 6 PM → first day weight = (24 - 18) / 24 = 0.25

This assumes events are uniformly distributed within the day. Not perfect, but much better than including the full day.

### The Benchmark Tests Both

For every pre-agg query, two variants are generated:
- **Unweighted**: Full partial day included (simple, biased)
- **Weighted**: Fractional weighting on first day (production-quality)

The on-demand approach has exact timestamp precision, so it serves as ground truth.
