-- benchmarks/cold-start/queries/dashboard-20.sql
--
-- 20 selective filter queries over the TPC-DS Iceberg fixture. Each
-- query is the kind of pattern a BI dashboard issues after a Trino
-- elastic scale-up: a column-projection + predicate over a fact table
-- + small dimension join.
--
-- Per benchmarks/cold-start/SPEC.md §Workload:
--   - Each query stresses the per-worker cache cold-start path.
--   - Selective enough to exercise predicate pushdown (Iceberg metadata
--     skip, page-index, bloom).
--   - Diverse enough that prefetch on query N does not warm queries
--     N+1..20 by accident.
--
-- Numbering is `d1`..`d20` to match the schema.json query_id pattern
-- `^d[0-9]{1,3}$`.
--
-- Catalog/schema is parameterised by the runner. The cold-start runner
-- substitutes `${CATALOG}.${SCHEMA}` at issue time so the same query
-- set runs against `cdp.tpcds_sf100` and `cdp_shelf.tpcds_sf100`.

-- d1: customer demographics narrow scan
SELECT count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.customer_demographics
WHERE cd_gender = 'F' AND cd_marital_status = 'S';

-- d2: store_sales date range
SELECT count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.store_sales
WHERE ss_sold_date_sk BETWEEN 2451180 AND 2451210;

-- d3: web_sales price filter
SELECT count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.web_sales
WHERE ws_net_paid > 1000;

-- d4: catalog_sales return amount
SELECT count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.catalog_sales
WHERE cs_net_paid > 5000 AND cs_ext_discount_amt > 100;

-- d5: store_sales × store dimension join
SELECT s.s_state, count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.store_sales ss
JOIN ${CATALOG}.${SCHEMA}.store      s  ON ss.ss_store_sk = s.s_store_sk
WHERE ss.ss_sold_date_sk BETWEEN 2451180 AND 2451210
GROUP BY s.s_state
LIMIT 50;

-- d6: web_sales × customer
SELECT c.c_birth_country, sum(ws.ws_net_paid) AS revenue
FROM ${CATALOG}.${SCHEMA}.web_sales ws
JOIN ${CATALOG}.${SCHEMA}.customer  c  ON ws.ws_bill_customer_sk = c.c_customer_sk
WHERE ws.ws_sold_date_sk BETWEEN 2451180 AND 2451210
GROUP BY c.c_birth_country
LIMIT 25;

-- d7: items above price threshold
SELECT i_category, count(*) AS items
FROM ${CATALOG}.${SCHEMA}.item
WHERE i_current_price > 50
GROUP BY i_category
ORDER BY items DESC
LIMIT 20;

-- d8: top web pages by impressions (small dim, range scan)
SELECT wp_url, count(*) AS hits
FROM ${CATALOG}.${SCHEMA}.web_page
WHERE wp_link_count > 100
GROUP BY wp_url
LIMIT 10;

-- d9: store_returns within window
SELECT count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.store_returns
WHERE sr_returned_date_sk BETWEEN 2451180 AND 2451210;

-- d10: catalog_returns × catalog_sales
SELECT cr_return_amount * 100 AS amt_cents
FROM ${CATALOG}.${SCHEMA}.catalog_returns
WHERE cr_return_amount > 500
LIMIT 100;

-- d11: customer_address narrow
SELECT ca_state, count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.customer_address
WHERE ca_country = 'United States'
GROUP BY ca_state
LIMIT 50;

-- d12: time_dim hour-of-day distribution
SELECT t_hour, count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.time_dim
WHERE t_meal_time IS NOT NULL
GROUP BY t_hour
LIMIT 24;

-- d13: web_returns by reason
SELECT wr_reason_sk, count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.web_returns
WHERE wr_returned_date_sk BETWEEN 2451180 AND 2451210
GROUP BY wr_reason_sk
LIMIT 20;

-- d14: ss_quantity histogram
SELECT ss_quantity, count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.store_sales
WHERE ss_quantity BETWEEN 1 AND 10
GROUP BY ss_quantity
ORDER BY ss_quantity;

-- d15: top promotions by discount
SELECT p_promo_id, p_discount_active
FROM ${CATALOG}.${SCHEMA}.promotion
WHERE p_discount_active = 'Y'
LIMIT 25;

-- d16: inventory snapshot
SELECT inv_warehouse_sk, sum(inv_quantity_on_hand) AS qty
FROM ${CATALOG}.${SCHEMA}.inventory
WHERE inv_date_sk = 2451180
GROUP BY inv_warehouse_sk
LIMIT 20;

-- d17: warehouse list
SELECT w_warehouse_id, w_warehouse_name
FROM ${CATALOG}.${SCHEMA}.warehouse
LIMIT 20;

-- d18: ship_mode codes
SELECT sm_type, count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.ship_mode
WHERE sm_type IS NOT NULL
GROUP BY sm_type
LIMIT 10;

-- d19: catalog_sales price > X
SELECT cs_call_center_sk, count(*) AS rows
FROM ${CATALOG}.${SCHEMA}.catalog_sales
WHERE cs_net_paid > 10000
GROUP BY cs_call_center_sk
LIMIT 20;

-- d20: revenue per category (heaviest of the 20)
SELECT i.i_category, sum(ws.ws_net_paid) AS revenue
FROM ${CATALOG}.${SCHEMA}.web_sales ws
JOIN ${CATALOG}.${SCHEMA}.item      i  ON ws.ws_item_sk = i.i_item_sk
WHERE ws.ws_sold_date_sk BETWEEN 2451180 AND 2451210
GROUP BY i.i_category
ORDER BY revenue DESC
LIMIT 10;
