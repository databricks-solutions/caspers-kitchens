-- 2-lakebase-issue-fair-refund
-- Run on: Lakebase (databricks_postgres)
--
-- Issues goodwill refunds for open complaints in the active city's latest
-- session. Refund amount is derived from this city's 90-day historical
-- averages (lakebase.bronze_hist_refunds), with a cold-chain
-- uplift. Inserts refunds, resolves the matching complaints, and returns
-- the refunds sent.

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
    FROM lakebase.bronze_hist_refunds
    GROUP BY city_id, kind
),
complainers AS (
    SELECT
        c.id AS complaint_id, c.order_id,
        o.session_id, o.city, o.kitchen, o.kind AS kind_label, o.cold,
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
        cm.session_id,
        cm.city,
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
    INSERT INTO refunds (order_id, amount, reason, session_id, city)
    SELECT order_id, refund_offer,
           'Goodwill refund $' || refund_offer || ' — ' || kitchen ||
           ' historical average for this location',
           session_id, city
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
