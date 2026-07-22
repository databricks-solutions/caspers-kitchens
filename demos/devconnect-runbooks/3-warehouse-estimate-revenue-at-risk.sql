-- 3-warehouse-estimate-revenue-at-risk
-- Run on: SQL warehouse ({catalog}-devconnect-ops) — Unity Catalog, read-only
--
-- Estimates revenue still at risk in the current demo run: open live orders
-- (from Lakebase CDF → lakebase.lb_orders_history) valued with
-- this city's historical average order value from orders.bronze_hist_orders.
-- Grouped by status × item kind (with ROLLUP totals).
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
hist_val AS (
    SELECT city_id, kind, ROUND(AVG(order_value), 2) AS avg_order_value
    FROM devconnect.orders.bronze_hist_orders
    GROUP BY city_id, kind
),
live_open AS (
    SELECT o.order_id, o.city, o.status, o.kind AS kind_label,
        CASE o.kind
            WHEN 'Hot food'  THEN 'hot'
            WHEN 'Groceries' THEN 'grocery'
            WHEN 'Ice cream' THEN 'ice'
            WHEN 'Frozen'    THEN 'frozen'
        END AS kind_code
    FROM live_orders o
    WHERE o.session_id = (SELECT session_id FROM latest)
      AND o.status IN ('placed', 'routing', 'enroute', 'stuck', 'late', 'rerouted')
)
SELECT
    lo.status,
    lo.kind_label AS item,
    COUNT(*) AS orders_at_risk,
    ROUND(SUM(COALESCE(h.avg_order_value, 20.00)), 2) AS est_revenue_at_risk
FROM live_open lo
LEFT JOIN hist_val h ON h.city_id = lo.city AND h.kind = lo.kind_code
GROUP BY ROLLUP (lo.status, lo.kind_label)
ORDER BY lo.status NULLS LAST, est_revenue_at_risk DESC;
