-- 1-lakebase-reroute-orders
-- Run on: Lakebase (databricks_postgres)
--
-- Closes the active city's choke-point crossing in route_policies, then
-- reroutes stuck cold-chain orders that are still within their delivery
-- window (late_min < max_delay_min) for the latest session. Returns the
-- orders that were flipped to status = 'rerouted'.

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
