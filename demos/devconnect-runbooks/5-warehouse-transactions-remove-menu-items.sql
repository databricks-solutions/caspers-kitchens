-- 5-warehouse-transactions-remove-menu-items
-- Run on: SQL warehouse ({catalog}-devconnect-ops) — Unity Catalog
--
-- Eighty-six burger buns for the active city in one atomic transaction:
-- zero kitchen_inventory qty AND flip menu_availability off together.
-- Run the three blocks in order: 5a (before) → 5 (atomic write) → 5b (after).
-- After 5b, inconsistent_rows must be 0 (no dish still marked available
-- when stock is zero).

-- ── 5a · BEFORE — burger bun stock + available menu dishes ──────────────────

WITH active AS (
    SELECT city_id FROM devconnect.metadata.demo_active_city WHERE id = 1
)
SELECT 'stock' AS what, SUM(i.qty) AS burger_buns_qty, CAST(NULL AS BIGINT) AS available_dishes
FROM   devconnect.metadata.kitchen_inventory i
JOIN   active a ON i.city_id = a.city_id
WHERE  i.ingredient = 'burger_buns'
UNION ALL
SELECT 'menu', CAST(NULL AS BIGINT), COUNT(*)
FROM   devconnect.metadata.menu_availability m
JOIN   active a ON m.city_id = a.city_id
WHERE  m.ingredient = 'burger_buns' AND m.available = true;

-- ── 5 · ATOMIC — zero stock AND remove from menu in one commit ──────────────

BEGIN ATOMIC
  UPDATE devconnect.metadata.kitchen_inventory i
  SET    qty = 0, updated_at = current_timestamp()
  WHERE  i.city_id = (SELECT city_id FROM devconnect.metadata.demo_active_city WHERE id = 1)
    AND  i.ingredient = 'burger_buns';

  UPDATE devconnect.metadata.menu_availability m
  SET    available = false, updated_at = current_timestamp()
  WHERE  m.city_id = (SELECT city_id FROM devconnect.metadata.demo_active_city WHERE id = 1)
    AND  m.ingredient = 'burger_buns';
END;

-- ── 5b · AFTER — inconsistent_rows must be 0 ────────────────────────────────

WITH active AS (
    SELECT city_id FROM devconnect.metadata.demo_active_city WHERE id = 1
)
SELECT
  (SELECT COALESCE(SUM(i.qty), 0)
     FROM devconnect.metadata.kitchen_inventory i
     JOIN active a ON i.city_id = a.city_id
     WHERE i.ingredient = 'burger_buns') AS burger_buns_qty,
  (SELECT COUNT(*)
     FROM devconnect.metadata.menu_availability m
     JOIN active a ON m.city_id = a.city_id
     WHERE m.ingredient = 'burger_buns' AND m.available = true) AS still_available,
  (SELECT COUNT(*)
     FROM devconnect.metadata.menu_availability m
     JOIN devconnect.metadata.kitchen_inventory i
       ON i.kitchen_id = m.kitchen_id AND i.ingredient = m.ingredient
     JOIN active a ON m.city_id = a.city_id
     WHERE m.available = true AND i.qty = 0) AS inconsistent_rows;
