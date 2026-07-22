-- 4-warehouse-compare-today-vs-normal
-- Run on: SQL warehouse ({catalog}-devconnect-ops) — Unity Catalog, read-only
--
-- Compares this demo run against a normal day for the active city:
-- cancel %, disrupted % (stuck/rerouted), and average lateness. Live side
-- comes from Lakebase CDF (lb_orders_history); baseline from
-- orders.bronze_hist_orders (90-day avg).
--
-- Requires: Lakebase CDF into lb_orders_history (manual setup; see README).

WITH active AS (
    SELECT city_id FROM devconnect.metadata.demo_active_city WHERE id = 1
),
live_orders AS (
    SELECT order_id, session_id, city, status, kind, late_min, updated_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _sort_by DESC) AS rn
        FROM devconnect.lakebase.lb_orders_history
    ) o
    WHERE rn = 1
      AND _pg_change_type IN ('insert', 'update_postimage')
),
latest AS (
    -- Prefer the active demo city; if no live orders there (stale demo_active_city),
    -- fall back to whichever city has the most recent order.
    SELECT session_id, city FROM (
        SELECT o.session_id, o.city,
               ROW_NUMBER() OVER (
                   ORDER BY CASE WHEN o.city = a.city_id THEN 0 ELSE 1 END, o.updated_at DESC
               ) AS rn
        FROM live_orders o
        CROSS JOIN active a
    ) WHERE rn = 1
),
live AS (
    SELECT
        COUNT(*) AS orders,
        ROUND(100.0 * AVG(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), 1) AS cancel_pct,
        ROUND(100.0 * AVG(CASE WHEN status IN ('stuck', 'rerouted') THEN 1 ELSE 0 END), 1) AS disrupted_pct,
        ROUND(AVG(late_min), 1) AS avg_late_min
    FROM live_orders
    WHERE session_id = (SELECT session_id FROM latest)
),
hist AS (
    SELECT
        ROUND(100.0 * AVG(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END), 1) AS cancel_pct,
        ROUND(100.0 * AVG(CASE WHEN disrupted THEN 1 ELSE 0 END), 1) AS disrupted_pct,
        ROUND(AVG(late_min), 1) AS avg_late_min
    FROM devconnect.orders.bronze_hist_orders h
    JOIN latest l ON h.city_id = l.city
)
SELECT 'today (this run)' AS period, l.orders, l.cancel_pct, l.disrupted_pct, l.avg_late_min FROM live l
UNION ALL
SELECT 'normal (90-day avg)' AS period, CAST(NULL AS BIGINT), h.cancel_pct, h.disrupted_pct, h.avg_late_min FROM hist h;
