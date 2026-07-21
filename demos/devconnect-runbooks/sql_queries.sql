-- QUERY 1 — Lakebase. Close crossing + reroute stuck cold orders still deliverable.

WITH active AS (
    SELECT COALESCE(
        NULLIF(TRIM((SELECT city FROM demo_config WHERE id = 1)), ''),
        (SELECT city FROM orders ORDER BY updated_at DESC LIMIT 1)
    ) AS city_id
)
INSERT INTO route_policies (city_id, crossing_name, status, alternate_crossing, updated_at)
SELECT a.city_id, c.bridge_name, 'closed', c.alt_name, NOW()
FROM active a
JOIN city_crossings c ON c.city_id = a.city_id
WHERE a.city_id IS NOT NULL
ON CONFLICT (city_id) DO UPDATE SET
  crossing_name      = EXCLUDED.crossing_name,
  status             = 'closed',
  alternate_crossing = EXCLUDED.alternate_crossing,
  updated_at         = NOW();

WITH latest AS (
    SELECT session_id FROM orders ORDER BY updated_at DESC LIMIT 1
)
UPDATE orders
SET    status = 'rerouted',
       updated_at = NOW()
WHERE  status = 'stuck'
  AND  cold = TRUE
  AND  late_min < max_delay_min
  AND  session_id = (SELECT session_id FROM latest)
RETURNING order_id, kitchen, kind, late_min, max_delay_min;

-- QUERY 2 — Lakebase. Goodwill refunds for open complaints (amount from this city's 90-day hist).

WITH active AS (
    SELECT COALESCE(
        NULLIF(TRIM((SELECT city FROM demo_config WHERE id = 1)), ''),
        (SELECT city FROM orders ORDER BY updated_at DESC LIMIT 1)
    ) AS city_id
),
latest AS (
    SELECT o.session_id
    FROM orders o
    WHERE o.city = (SELECT city_id FROM active)
    ORDER BY o.updated_at DESC
    LIMIT 1
),
hist_loc AS (
    SELECT
        city_id,
        kind,
        ROUND(AVG(refund_amount)::numeric, 2)                                       AS avg_refund,
        ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY refund_amount)::numeric, 2) AS p90_refund
    FROM catastrophe_lakebase.catastrophe_hist_refunds
    GROUP BY city_id, kind
),
complainers AS (
    SELECT
        c.id AS complaint_id, c.order_id,
        o.city, o.kitchen, o.kind AS kind_label, o.cold,
        CASE o.kind
            WHEN 'Hot food'  THEN 'hot'
            WHEN 'Groceries' THEN 'grocery'
            WHEN 'Ice cream' THEN 'ice'
            WHEN 'Frozen'    THEN 'frozen'
        END AS kind_code
    FROM complaints c
    JOIN orders o ON o.order_id = c.order_id
    WHERE c.resolved_at IS NULL
      AND o.session_id = (SELECT session_id FROM latest)
),
offer AS (
    SELECT
        cm.complaint_id,
        cm.order_id,
        cm.kitchen,
        ROUND(
            LEAST(
                GREATEST(
                    COALESCE(h.avg_refund, 12.00) * (CASE WHEN cm.cold THEN 1.25 ELSE 1.00 END),
                    5.00
                ),
                COALESCE(h.p90_refund, 45.00)
            ), 2
        ) AS refund_offer
    FROM complainers cm
    LEFT JOIN hist_loc h
           ON h.city_id = cm.city
          AND h.kind    = cm.kind_code
),
ins AS (
    INSERT INTO refunds (order_id, amount, reason)
    SELECT order_id, refund_offer,
           'Goodwill refund $' || refund_offer || ' — ' || kitchen ||
           ' historical average for this location'
    FROM offer
    RETURNING order_id, amount
),
res AS (
    UPDATE complaints c
    SET resolution  = 'Apology + $' || o.refund_offer || ' refund (historical avg)',
        resolved_at = NOW()
    FROM offer o
    WHERE c.id = o.complaint_id
      AND c.resolved_at IS NULL
    RETURNING c.order_id
)
SELECT order_id, amount AS refund_sent FROM ins ORDER BY amount DESC;

-- QUERY 3 — SQL warehouse (UC). Read-only. How much revenue is at risk in this run, right now?
-- Live orders: Lakebase CDF (public.orders → lb_orders_history). History: simulator.catastrophe_hist_orders.

WITH active AS (
    SELECT city_id FROM devconnect.simulator.demo_active_city WHERE id = 1
),
live_orders AS (
    SELECT order_id, session_id, city, status, kind, late_min, updated_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _sort_by DESC) AS rn
        FROM devconnect.catastrophe_lakebase.lb_orders_history
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
    FROM devconnect.simulator.catastrophe_hist_orders
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

-- QUERY 4 — SQL warehouse (UC). Read-only. How bad is today? This run vs a normal day in this city.
-- Live orders: Lakebase CDF (public.orders → lb_orders_history). History: simulator.catastrophe_hist_orders.

WITH active AS (
    SELECT city_id FROM devconnect.simulator.demo_active_city WHERE id = 1
),
live_orders AS (
    SELECT order_id, session_id, city, status, kind, late_min, updated_at
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _sort_by DESC) AS rn
        FROM devconnect.catastrophe_lakebase.lb_orders_history
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
    FROM devconnect.simulator.catastrophe_hist_orders h
    JOIN latest l ON h.city_id = l.city
)
SELECT 'today (this run)' AS period, l.orders, l.cancel_pct, l.disrupted_pct, l.avg_late_min FROM live l
UNION ALL
SELECT 'normal (90-day avg)' AS period, CAST(NULL AS BIGINT), h.cancel_pct, h.disrupted_pct, h.avg_late_min FROM hist h;

-- QUERY 5a — SQL warehouse (UC). BEFORE mozzarella stock + menu.

WITH active AS (
    SELECT city_id FROM devconnect.simulator.demo_active_city WHERE id = 1
)
SELECT 'stock' AS what, SUM(i.qty) AS mozzarella_qty, CAST(NULL AS BIGINT) AS available_dishes
FROM   devconnect.simulator.kitchen_inventory i
JOIN   active a ON i.city_id = a.city_id
WHERE  i.ingredient = 'mozzarella'
UNION ALL
SELECT 'menu', CAST(NULL AS BIGINT), COUNT(*)
FROM   devconnect.simulator.menu_availability m
JOIN   active a ON m.city_id = a.city_id
WHERE  m.ingredient = 'mozzarella' AND m.available = true;

-- QUERY 5 — SQL warehouse (UC). Zero stock AND flip menu in one atomic commit.

BEGIN ATOMIC
  UPDATE devconnect.simulator.kitchen_inventory i
  SET    qty = 0, updated_at = current_timestamp()
  WHERE  i.city_id = (SELECT city_id FROM devconnect.simulator.demo_active_city WHERE id = 1)
    AND  i.ingredient = 'mozzarella';

  UPDATE devconnect.simulator.menu_availability m
  SET    available = false, updated_at = current_timestamp()
  WHERE  m.city_id = (SELECT city_id FROM devconnect.simulator.demo_active_city WHERE id = 1)
    AND  m.ingredient = 'mozzarella';
END;

-- QUERY 5b — SQL warehouse (UC). AFTER — inconsistent_rows must be 0.

WITH active AS (
    SELECT city_id FROM devconnect.simulator.demo_active_city WHERE id = 1
)
SELECT
  (SELECT COALESCE(SUM(i.qty), 0)
     FROM devconnect.simulator.kitchen_inventory i
     JOIN active a ON i.city_id = a.city_id
     WHERE i.ingredient = 'mozzarella') AS mozzarella_qty,
  (SELECT COUNT(*)
     FROM devconnect.simulator.menu_availability m
     JOIN active a ON m.city_id = a.city_id
     WHERE m.ingredient = 'mozzarella' AND m.available = true) AS still_available,
  (SELECT COUNT(*)
     FROM devconnect.simulator.menu_availability m
     JOIN devconnect.simulator.kitchen_inventory i
       ON i.kitchen_id = m.kitchen_id AND i.ingredient = m.ingredient
     JOIN active a ON m.city_id = a.city_id
     WHERE m.available = true AND i.qty = 0) AS inconsistent_rows;
