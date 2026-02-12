-- Initial schema for refund_decisions table
-- Note: public.recommendations table is created by Lakebase/streaming pipeline

CREATE TABLE IF NOT EXISTS public.refund_decisions (
    id BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    decided_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    amount_usd NUMERIC(10,2) NOT NULL CHECK (amount_usd >= 0),
    refund_class TEXT NOT NULL CHECK (refund_class IN ('none','partial','full')),
    reason TEXT NOT NULL,
    decided_by TEXT,
    source_suggestion JSONB
);

CREATE INDEX IF NOT EXISTS idx_refund_decisions_order_id ON public.refund_decisions(order_id);
