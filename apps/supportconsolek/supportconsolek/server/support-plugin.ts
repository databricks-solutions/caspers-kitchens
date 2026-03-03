import { Plugin } from "@databricks/appkit";
import type { IAppRouter } from "@databricks/appkit";
import { WorkspaceClient } from "@databricks/sdk-experimental";
import type { Pool } from "pg";
import { createLakebasePool } from "./lib/lakebase.js";

type Json = Record<string, unknown>;

interface ActionBody {
  support_request_id: string;
  order_id: string;
  user_id?: string | null;
  action_type: "apply_refund" | "apply_credit";
  amount_usd: number;
  actor?: string | null;
  payload?: Json;
}

interface ReplyBody {
  support_request_id: string;
  order_id: string;
  user_id?: string | null;
  message_text: string;
  sent_by?: string | null;
}

interface RegenerateBody {
  support_request_id: string;
  user_id?: string | null;
  order_id: string;
  operator_context?: string | null;
  actor?: string | null;
  current_report?: Record<string, unknown> | null;
}

interface RatingBody {
  support_request_id: string;
  order_id: string;
  user_id?: string | null;
  rating: "thumbs_up" | "thumbs_down";
  reason_code?: string | null;
  feedback_notes?: string | null;
  actor?: string | null;
}

type CaseStatus = "pending" | "in_progress" | "done" | "blocked";

const DEFAULT_REPORT = {
  credit_recommendation: null,
  refund_recommendation: null,
  draft_response: "",
  past_interactions_summary: "",
  order_details_summary: "",
  decision_confidence: "medium",
  escalation_flag: false,
};

const FIRST_NAMES = [
  "Alex",
  "Sam",
  "Jordan",
  "Taylor",
  "Morgan",
  "Casey",
  "Riley",
  "Avery",
  "Cameron",
  "Quinn",
];

const LAST_NAMES = [
  "Parker",
  "Hayes",
  "Brooks",
  "Reed",
  "Foster",
  "Bailey",
  "Carter",
  "Gray",
  "Miller",
  "Price",
];

function fakeDisplayName(userId: string | null | undefined): string | null {
  if (!userId) return null;
  let hash = 0;
  for (let i = 0; i < userId.length; i += 1) {
    hash = (hash * 31 + userId.charCodeAt(i)) >>> 0;
  }
  const first = FIRST_NAMES[hash % FIRST_NAMES.length];
  const last = LAST_NAMES[Math.floor(hash / FIRST_NAMES.length) % LAST_NAMES.length];
  return `${first} ${last}`;
}

function parseAgentReport(value: unknown): Record<string, unknown> {
  try {
    if (typeof value === "string") {
      const parsed = JSON.parse(value) as Record<string, unknown>;
      return { ...DEFAULT_REPORT, ...parsed };
    }
    if (value && typeof value === "object") {
      return { ...DEFAULT_REPORT, ...(value as Record<string, unknown>) };
    }
  } catch {
    // fall through to default report
  }
  return { ...DEFAULT_REPORT };
}

function sanitizeDraftResponse(
  draft: unknown,
  userId: string | null | undefined,
): string {
  const base = typeof draft === "string" ? draft : "";
  const displayName = fakeDisplayName(userId) ?? "there";

  // Remove internal identifiers from customer-facing draft text.
  return base
    .replace(/\buser-\d+\b/gi, displayName)
    .replace(/\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b/gi, "your order")
    .replace(/\b[0-9a-f]{32}\b/gi, "your order")
    .replace(/\border\s+[0-9a-f]{8,}\b/gi, "order")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function toNullableNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) {
      return parsed;
    }
  }
  return null;
}

function normalizeRecommendation(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const raw = value as Record<string, unknown>;
  const amount =
    toNullableNumber(raw.amount_usd) ??
    toNullableNumber(raw.amount) ??
    toNullableNumber(raw.credit_amount) ??
    toNullableNumber(raw.refund_amount);

  const reasonCandidates = [
    raw.reason,
    raw.credit_reason,
    raw.refund_reason,
  ];
  const reason = reasonCandidates.find((r) => typeof r === "string" && r.trim().length > 0) as
    | string
    | undefined;

  // Keep original fields for traceability, but expose normalized UI fields.
  return {
    ...raw,
    amount_usd: amount,
    reason: reason ?? raw.reason ?? "",
  };
}

function normalizeReportForUi(
  rawReport: Record<string, unknown>,
  userId: string | null | undefined,
): Record<string, unknown> {
  return {
    ...rawReport,
    credit_recommendation: normalizeRecommendation(rawReport.credit_recommendation),
    refund_recommendation: normalizeRecommendation(rawReport.refund_recommendation),
    draft_response: sanitizeDraftResponse(rawReport.draft_response, userId),
  };
}

function extractResponseText(response: unknown): string {
  if (!response || typeof response !== "object") {
    throw new Error("Invalid serving response");
  }
  const output = (response as { output?: unknown }).output;
  if (!Array.isArray(output)) {
    throw new Error("Serving response missing output");
  }
  for (const message of output) {
    if (!message || typeof message !== "object") continue;
    const content = (message as { content?: unknown }).content;
    if (!Array.isArray(content)) continue;
    for (const item of content) {
      if (!item || typeof item !== "object") continue;
      const text = (item as { text?: unknown }).text;
      if (typeof text === "string" && text.trim().length > 0) {
        return text;
      }
    }
  }
  throw new Error("Serving response text not found");
}

function extractErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error ?? "unknown_error");
}

function isServingPermissionError(error: unknown): boolean {
  const message = extractErrorMessage(error).toLowerCase();
  return (
    message.includes("do not have permission")
    || message.includes("permission denied")
    || message.includes("not authorized")
    || message.includes("unauthorized")
  );
}

function isPermissionDeniedError(error: unknown): boolean {
  const message = extractErrorMessage(error).toLowerCase();
  return message.includes("permission denied") || message.includes("must be owner");
}

async function fetchSupportRequestText(pool: Pool, supportRequestId: string): Promise<string | null> {
  const tableCandidates = [
    "support.support_agent_reports_sync",
    "support.raw_support_requests_sync",
    "support.raw_support_requests",
  ];

  for (const tableName of tableCandidates) {
    try {
      const result = await pool.query(
        `SELECT request_text
         FROM ${tableName}
         WHERE support_request_id = $1
         ORDER BY ts DESC
         LIMIT 1`,
        [supportRequestId],
      );
      const value = result.rows[0]?.request_text;
      if (typeof value === "string" && value.trim().length > 0) {
        return value;
      }
    } catch (error) {
      const message = extractErrorMessage(error).toLowerCase();
      if (
        message.includes("does not exist")
        || message.includes("undefined table")
        || message.includes("undefined column")
        || message.includes("permission denied")
      ) {
        continue;
      }
      throw error;
    }
  }

  return null;
}

function createRegenerationPrompt(params: {
  supportRequestId: string;
  userId: string | null | undefined;
  orderId: string;
  operatorContext: string | null | undefined;
  currentReport: Record<string, unknown> | null | undefined;
}): string {
  const {
    supportRequestId,
    userId,
    orderId,
    operatorContext,
    currentReport,
  } = params;

  const baseContext = {
    support_request_id: supportRequestId,
    user_id: userId ?? "",
    order_id: orderId,
    previous_report: currentReport ?? {},
    operator_context: operatorContext ?? "",
  };

  return [
    "You are the Caspers support triage agent.",
    "Re-evaluate this support case using the previous report and operator context.",
    "Return strictly valid JSON only with keys:",
    "support_request_id, user_id, order_id, credit_recommendation, refund_recommendation, draft_response, past_interactions_summary, order_details_summary, decision_confidence, escalation_flag.",
    "Recommendation objects must use keys {\"amount_usd\": number, \"reason\": string} or null.",
    `Case payload: ${JSON.stringify(baseContext)}`,
  ].join("\n");
}

function createFallbackRegeneratedReport(params: {
  currentReport: Record<string, unknown> | null | undefined;
  operatorContext: string | null | undefined;
  supportRequestId: string;
  userId: string | null | undefined;
  orderId: string;
}): Record<string, unknown> {
  const report = parseAgentReport(params.currentReport ?? {});
  const operatorContext = (params.operatorContext ?? "").trim();
  const existingDraft = typeof report.draft_response === "string" ? report.draft_response.trim() : "";

  if (operatorContext.length > 0) {
    const contextLine = `Operator context: ${operatorContext}`;
    report.draft_response = existingDraft.length > 0
      ? `${existingDraft}\n\n${contextLine}`
      : contextLine;
  } else if (!existingDraft) {
    report.draft_response = "Thanks for contacting us. We reviewed your case and prepared a concrete resolution proposal below.";
  }

  report.support_request_id = params.supportRequestId;
  report.user_id = params.userId ?? "";
  report.order_id = params.orderId;
  report.regeneration_mode = "fallback_permission_denied";

  return normalizeReportForUi(report, params.userId);
}

function deriveCaseStatus(params: {
  status: unknown;
  hasRefund: unknown;
  hasCredit: unknown;
  actionCount: unknown;
  replyCount: unknown;
  regenCount: unknown;
}): CaseStatus {
  const status = typeof params.status === "string" ? params.status : null;
  if (status === "blocked") return "blocked";
  if (params.hasRefund === true || params.hasCredit === true || status === "done") return "done";

  const activityCount =
    Number(params.actionCount ?? 0) +
    Number(params.replyCount ?? 0) +
    Number(params.regenCount ?? 0);
  return activityCount > 0 || status === "in_progress" ? "in_progress" : "pending";
}

function deriveNextAction(params: {
  caseStatus: CaseStatus;
  hasReply: boolean;
  hasRefund: boolean;
  hasCredit: boolean;
}): string {
  if (params.caseStatus === "pending") return "review_report";
  if (params.caseStatus === "blocked") return "investigate_blocker";
  if (params.caseStatus === "done") {
    return params.hasReply ? "monitor" : "send_customer_reply";
  }
  if (params.hasReply && !params.hasRefund && !params.hasCredit) return "apply_resolution_or_regenerate";
  if (!params.hasReply && (params.hasRefund || params.hasCredit)) return "send_customer_reply";
  return "continue_investigation";
}

export class SupportPlugin extends Plugin {
  public name = "support";
  protected envVars: string[] = [];
  private pool: Pool | null = null;
  private setupError: string | null = null;
  private workspaceClient: WorkspaceClient | null = null;
  private supportAgentEndpoint = process.env.SUPPORT_AGENT_ENDPOINT_NAME ?? "caspers_support_agent";

  static manifest = {
    name: "support",
    displayName: "Support Plugin",
    description: "Support request APIs backed by Lakebase",
    resources: {
      required: [],
      optional: [],
    },
  };

  async setup(): Promise<void> {
    try {
      this.pool = await createLakebasePool();
      this.workspaceClient = new WorkspaceClient({
        host: process.env.DATABRICKS_HOST,
        ...(process.env.DATABRICKS_CONFIG_PROFILE && {
          profile: process.env.DATABRICKS_CONFIG_PROFILE,
        }),
      });
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS support.operator_regenerated_reports (
           regenerated_report_id BIGSERIAL PRIMARY KEY,
           support_request_id TEXT NOT NULL,
           user_id TEXT,
           order_id TEXT NOT NULL,
           operator_context TEXT,
           agent_response JSONB NOT NULL,
           actor TEXT,
           created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
         )`,
      );
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS support.request_status (
           support_request_id TEXT PRIMARY KEY,
           status TEXT NOT NULL DEFAULT 'pending',
           assigned_to TEXT,
           updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
           last_action TEXT,
           notes TEXT
         )`,
      );
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS support.response_ratings (
           rating_id BIGSERIAL PRIMARY KEY,
           support_request_id TEXT NOT NULL,
           order_id TEXT NOT NULL,
           user_id TEXT,
           rating TEXT NOT NULL CHECK (rating IN ('thumbs_up', 'thumbs_down')),
           reason_code TEXT,
           feedback_notes TEXT,
           actor TEXT,
           created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
           UNIQUE (support_request_id)
         )`,
      );
      await this.pool.query(
        `CREATE UNIQUE INDEX IF NOT EXISTS response_ratings_support_request_id_uq
         ON support.response_ratings (support_request_id)`,
      );
    } catch (error) {
      this.setupError = error instanceof Error ? error.message : String(error);
      console.error("[support] setup failed:", this.setupError);
    }
  }

  injectRoutes(router: IAppRouter): void {
    const withJsonError = (
      handler: (req: any, res: any) => Promise<void>,
    ) => async (req: any, res: any) => {
      try {
        await handler(req, res);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error("[support] route error:", message);
        if (!res.headersSent) {
          res.status(500).json({ error: "internal_error", message });
        }
      }
    };

    router.get("/healthz", withJsonError(async (_req, res) => {
      if (!this.pool) {
        res
          .status(503)
          .json({ ok: false, error: "DB not ready", message: this.setupError });
        return;
      }
      await this.pool.query("SELECT 1");
      res.json({ ok: true });
    }));

    router.get("/summary", withJsonError(async (_req, res) => {
      if (!this.pool) {
        res
          .status(503)
          .json({ error: "DB not ready", message: this.setupError });
        return;
      }
      const [requests, actions, replies] = await Promise.all([
        this.pool.query(
          `WITH combined AS (
             SELECT support_request_id, ts AS event_ts
             FROM support.support_agent_reports_sync
             UNION ALL
             SELECT support_request_id, created_at AS event_ts
             FROM support.operator_regenerated_reports
           ),
           latest AS (
             SELECT DISTINCT ON (support_request_id) support_request_id, event_ts
             FROM combined
             ORDER BY support_request_id, event_ts DESC
           )
           SELECT COUNT(*)::int AS c FROM latest`,
        ),
        this.pool.query("SELECT COUNT(*)::int AS c FROM support.operator_actions"),
        this.pool.query("SELECT COUNT(*)::int AS c FROM support.support_replies"),
      ]);
      res.json({
        requests: requests.rows[0]?.c ?? 0,
        actions: actions.rows[0]?.c ?? 0,
        replies: replies.rows[0]?.c ?? 0,
      });
    }));

    router.get("/requests", withJsonError(async (req, res) => {
      if (!this.pool) {
        res
          .status(503)
          .json({ error: "DB not ready", message: this.setupError });
        return;
      }
      const limit = Number(req.query.limit ?? 50);
      const offset = Number(req.query.offset ?? 0);

      const rows = await this.pool.query(
        `WITH combined AS (
           SELECT support_request_id, user_id, order_id, ts, agent_response::jsonb AS agent_response, ts AS event_ts, 'sync'::text AS report_source
           FROM support.support_agent_reports_sync
           UNION ALL
           SELECT support_request_id, user_id, order_id, created_at AS ts, agent_response, created_at AS event_ts, 'regenerated'::text AS report_source
           FROM support.operator_regenerated_reports
         ),
         latest AS (
           SELECT DISTINCT ON (support_request_id)
             support_request_id, user_id, order_id, ts, agent_response, event_ts, report_source
           FROM combined
           ORDER BY support_request_id, event_ts DESC
         ),
         action_agg AS (
           SELECT
             support_request_id,
             COUNT(*)::int AS action_count,
             COALESCE(BOOL_OR(action_type = 'apply_refund'), false) AS has_refund,
             COALESCE(BOOL_OR(action_type = 'apply_credit'), false) AS has_credit,
             COALESCE(BOOL_OR(action_type = 'send_reply'), false) AS has_reply_action,
             MAX(created_at) AS last_action_at,
             (ARRAY_AGG(action_type ORDER BY created_at DESC))[1] AS last_action_type
           FROM support.operator_actions
           GROUP BY support_request_id
         ),
         reply_agg AS (
           SELECT
             support_request_id,
             COUNT(*)::int AS reply_count,
             MAX(created_at) AS last_reply_at
           FROM support.support_replies
           GROUP BY support_request_id
         ),
         regen_agg AS (
           SELECT
             support_request_id,
             COUNT(*)::int AS regen_count,
             MAX(created_at) AS last_regen_at
           FROM support.operator_regenerated_reports
           GROUP BY support_request_id
         )
         SELECT
           latest.support_request_id,
           latest.user_id,
           latest.order_id,
           latest.ts,
           latest.agent_response,
           latest.report_source,
           COALESCE(action_agg.action_count, 0) AS action_count,
           COALESCE(reply_agg.reply_count, 0) AS reply_count,
           COALESCE(regen_agg.regen_count, 0) AS regen_count,
           COALESCE(action_agg.has_refund, false) AS has_refund,
           COALESCE(action_agg.has_credit, false) AS has_credit,
           (COALESCE(reply_agg.reply_count, 0) > 0 OR COALESCE(action_agg.has_reply_action, false)) AS has_reply,
           action_agg.last_action_type,
           rs.status,
           rs.last_action,
           (
             SELECT MAX(event_time) FROM (
               VALUES
                 (latest.ts),
                 (action_agg.last_action_at),
                 (reply_agg.last_reply_at),
                 (regen_agg.last_regen_at),
                 (rs.updated_at)
             ) AS events(event_time)
           ) AS last_event_at
         FROM latest
         LEFT JOIN action_agg ON action_agg.support_request_id = latest.support_request_id
         LEFT JOIN reply_agg ON reply_agg.support_request_id = latest.support_request_id
         LEFT JOIN regen_agg ON regen_agg.support_request_id = latest.support_request_id
         LEFT JOIN support.request_status rs ON rs.support_request_id = latest.support_request_id
         ORDER BY latest.ts DESC
         LIMIT $1 OFFSET $2`,
        [limit, offset],
      );
      const total = await this.pool.query(
        `WITH combined AS (
           SELECT support_request_id, ts AS event_ts
           FROM support.support_agent_reports_sync
           UNION ALL
           SELECT support_request_id, created_at AS event_ts
           FROM support.operator_regenerated_reports
         ),
         latest AS (
           SELECT DISTINCT ON (support_request_id) support_request_id, event_ts
           FROM combined
           ORDER BY support_request_id, event_ts DESC
         )
         SELECT COUNT(*)::int AS c FROM latest`,
      );

      res.json({
        items: rows.rows.map((r) => {
          const caseStatus = deriveCaseStatus({
            status: r.status,
            hasRefund: r.has_refund,
            hasCredit: r.has_credit,
            actionCount: r.action_count,
            replyCount: r.reply_count,
            regenCount: r.regen_count,
          });
          const hasReply = r.has_reply === true;
          const hasRefund = r.has_refund === true;
          const hasCredit = r.has_credit === true;

          return {
            support_request_id: r.support_request_id,
            user_id: r.user_id,
            user_display_name: fakeDisplayName(r.user_id),
            order_id: r.order_id,
            ts: r.ts,
            report: normalizeReportForUi(parseAgentReport(r.agent_response), r.user_id),
            case_state: {
              case_status: caseStatus,
              next_action: deriveNextAction({
                caseStatus,
                hasReply,
                hasRefund,
                hasCredit,
              }),
              has_reply: hasReply,
              has_refund: hasRefund,
              has_credit: hasCredit,
              action_count: Number(r.action_count ?? 0),
              reply_count: Number(r.reply_count ?? 0),
              regen_count: Number(r.regen_count ?? 0),
              last_action_type: r.last_action_type ?? r.last_action ?? null,
              last_event_at: r.last_event_at ?? r.ts,
              latest_report_source: r.report_source ?? "sync",
            },
          };
        }),
        total: total.rows[0]?.c ?? 0,
        limit,
        offset,
      });
    }));

    router.get("/requests/:id", withJsonError(async (req, res) => {
      if (!this.pool) {
        res
          .status(503)
          .json({ error: "DB not ready", message: this.setupError });
        return;
      }
      const sid = req.params.id;
      const request = await this.pool.query(
        `WITH combined AS (
           SELECT support_request_id, user_id, order_id, ts, agent_response::jsonb AS agent_response, ts AS event_ts, 'sync'::text AS report_source
           FROM support.support_agent_reports_sync
           WHERE support_request_id = $1
           UNION ALL
           SELECT support_request_id, user_id, order_id, created_at AS ts, agent_response, created_at AS event_ts, 'regenerated'::text AS report_source
           FROM support.operator_regenerated_reports
           WHERE support_request_id = $1
         )
         SELECT support_request_id, user_id, order_id, ts, agent_response, report_source
         FROM combined
         ORDER BY event_ts DESC
         LIMIT 1`,
        [sid],
      );
      if (request.rows.length === 0) {
        res.status(404).json({ error: "Not found" });
        return;
      }

      let ratingsRows: Array<Record<string, unknown>> = [];
      try {
        const ratingsResult = await this.pool.query(
          `SELECT rating_id, rating, reason_code, feedback_notes, actor, created_at
           FROM support.response_ratings
           WHERE support_request_id = $1
           ORDER BY created_at DESC`,
          [sid],
        );
        ratingsRows = ratingsResult.rows;
      } catch (error) {
        if (isPermissionDeniedError(error)) {
          console.warn("[support] ratings unavailable for this principal:", extractErrorMessage(error));
        } else {
          throw error;
        }
      }

      const [actions, replies, regenerations, statusRow] = await Promise.all([
        this.pool.query(
          `SELECT action_id, action_type, amount_usd, payload, status, actor, created_at
           FROM support.operator_actions
           WHERE support_request_id = $1
           ORDER BY created_at DESC`,
          [sid],
        ),
        this.pool.query(
          `SELECT reply_id, message_text, sent_by, created_at
           FROM support.support_replies
           WHERE support_request_id = $1
           ORDER BY created_at DESC`,
          [sid],
        ),
        this.pool.query(
          `SELECT regenerated_report_id, operator_context, actor, created_at, agent_response
           FROM support.operator_regenerated_reports
           WHERE support_request_id = $1
           ORDER BY created_at DESC`,
          [sid],
        ),
        this.pool.query(
          `SELECT status, last_action, updated_at
           FROM support.request_status
           WHERE support_request_id = $1
           LIMIT 1`,
          [sid],
        ),
      ]);

      const row = request.rows[0];
      const requestText = await fetchSupportRequestText(this.pool, sid);
      const hasRefund = actions.rows.some((a) => a.action_type === "apply_refund");
      const hasCredit = actions.rows.some((a) => a.action_type === "apply_credit");
      const hasReply = replies.rows.length > 0 || actions.rows.some((a) => a.action_type === "send_reply");
      const status = statusRow.rows[0]?.status;
      const caseStatus = deriveCaseStatus({
        status,
        hasRefund,
        hasCredit,
        actionCount: actions.rows.length,
        replyCount: replies.rows.length,
        regenCount: regenerations.rows.length,
      });

      const timeline = [
        { event_type: "report_generated", event_at: row.ts, actor: null, details: { source: row.report_source } },
        ...regenerations.rows.map((rr) => ({
          event_type: "report_regenerated",
          event_at: rr.created_at,
          actor: rr.actor ?? null,
          details: { operator_context: rr.operator_context ?? null },
        })),
        ...replies.rows.map((rp) => ({
          event_type: "reply_sent",
          event_at: rp.created_at,
          actor: rp.sent_by ?? null,
          details: { message_text: rp.message_text ?? "" },
        })),
        ...actions.rows.map((ac) => ({
          event_type: ac.action_type,
          event_at: ac.created_at,
          actor: ac.actor ?? null,
          details: {
            amount_usd: ac.amount_usd ?? null,
            status: ac.status ?? null,
          },
        })),
        ...ratingsRows.map((rt) => ({
          event_type: "response_rated",
          event_at: rt.created_at as string,
          actor: (rt.actor as string | null) ?? null,
          details: {
            rating: rt.rating as string,
            reason_code: (rt.reason_code as string | null) ?? null,
          },
        })),
      ].sort(
        (a, b) =>
          new Date(String(b.event_at)).getTime() - new Date(String(a.event_at)).getTime(),
      );

      res.json({
        support_request_id: row.support_request_id,
        user_id: row.user_id,
        user_display_name: fakeDisplayName(row.user_id),
        order_id: row.order_id,
        ts: row.ts,
        request_text: requestText,
        report: normalizeReportForUi(parseAgentReport(row.agent_response), row.user_id),
        actions: actions.rows,
        replies: replies.rows,
        ratings: ratingsRows,
        latest_rating: ratingsRows[0] ?? null,
        regenerations: regenerations.rows.map((rr) => ({
          regenerated_report_id: rr.regenerated_report_id,
          operator_context: rr.operator_context,
          actor: rr.actor,
          created_at: rr.created_at,
          report: normalizeReportForUi(parseAgentReport(rr.agent_response), row.user_id),
        })),
        case_state: {
          case_status: caseStatus,
          next_action: deriveNextAction({
            caseStatus,
            hasReply,
            hasRefund,
            hasCredit,
          }),
          has_reply: hasReply,
          has_refund: hasRefund,
          has_credit: hasCredit,
          action_count: actions.rows.length,
          reply_count: replies.rows.length,
          regen_count: regenerations.rows.length,
          last_action_type: statusRow.rows[0]?.last_action ?? actions.rows[0]?.action_type ?? null,
          last_event_at:
            timeline[0]?.event_at ??
            statusRow.rows[0]?.updated_at ??
            row.ts,
          latest_report_source: row.report_source ?? "sync",
        },
        timeline,
      });
    }));

    router.post("/actions", withJsonError(async (req, res) => {
      if (!this.pool) {
        res
          .status(503)
          .json({ error: "DB not ready", message: this.setupError });
        return;
      }
      const body = req.body as ActionBody;
      if (
        !body?.support_request_id ||
        !body?.order_id ||
        !body?.action_type ||
        typeof body.amount_usd !== "number"
      ) {
        res.status(400).json({ error: "Invalid payload" });
        return;
      }

      const inserted = await this.pool.query(
        `INSERT INTO support.operator_actions
           (support_request_id, order_id, user_id, action_type, amount_usd, payload, status, actor)
         VALUES ($1,$2,$3,$4,$5,$6::jsonb,'recorded',$7)
         RETURNING action_id, created_at`,
        [
          body.support_request_id,
          body.order_id,
          body.user_id ?? null,
          body.action_type,
          body.amount_usd,
          JSON.stringify(body.payload ?? {}),
          body.actor ?? null,
        ],
      );

      const nextStatus = body.action_type === "apply_refund" || body.action_type === "apply_credit"
        ? "done"
        : "in_progress";

      await this.pool.query(
        `INSERT INTO support.request_status (support_request_id, status, updated_at, last_action)
         VALUES ($1, $2, NOW(), $3)
         ON CONFLICT (support_request_id) DO UPDATE
         SET status = EXCLUDED.status,
             updated_at = EXCLUDED.updated_at,
             last_action = EXCLUDED.last_action`,
        [body.support_request_id, nextStatus, body.action_type],
      );

      res.status(201).json(inserted.rows[0]);
    }));

    router.post("/replies", withJsonError(async (req, res) => {
      if (!this.pool) {
        res
          .status(503)
          .json({ error: "DB not ready", message: this.setupError });
        return;
      }
      const body = req.body as ReplyBody;
      if (!body?.support_request_id || !body?.order_id || !body?.message_text) {
        res.status(400).json({ error: "Invalid payload" });
        return;
      }

      const inserted = await this.pool.query(
        `INSERT INTO support.support_replies
           (support_request_id, order_id, user_id, message_text, sent_by)
         VALUES ($1,$2,$3,$4,$5)
         RETURNING reply_id, created_at`,
        [
          body.support_request_id,
          body.order_id,
          body.user_id ?? null,
          body.message_text,
          body.sent_by ?? null,
        ],
      );

      await this.pool.query(
        `INSERT INTO support.operator_actions
           (support_request_id, order_id, user_id, action_type, payload, status, actor)
         VALUES ($1,$2,$3,'send_reply',$4::jsonb,'recorded',$5)`,
        [
          body.support_request_id,
          body.order_id,
          body.user_id ?? null,
          JSON.stringify({ message_text: body.message_text }),
          body.sent_by ?? null,
        ],
      );

      await this.pool.query(
        `INSERT INTO support.request_status (support_request_id, status, updated_at, last_action)
         VALUES ($1, 'in_progress', NOW(), 'send_reply')
         ON CONFLICT (support_request_id) DO UPDATE
         SET status = EXCLUDED.status,
             updated_at = EXCLUDED.updated_at,
             last_action = EXCLUDED.last_action`,
        [body.support_request_id],
      );

      res.status(201).json(inserted.rows[0]);
    }));

    router.post("/ratings", withJsonError(async (req, res) => {
      if (!this.pool) {
        res
          .status(503)
          .json({ error: "DB not ready", message: this.setupError });
        return;
      }
      const body = req.body as RatingBody;
      if (!body?.support_request_id || !body?.order_id || !body?.rating) {
        res.status(400).json({ error: "Invalid payload" });
        return;
      }
      if (body.rating !== "thumbs_up" && body.rating !== "thumbs_down") {
        res.status(400).json({ error: "Invalid rating value" });
        return;
      }

      let inserted;
      try {
        inserted = await this.pool.query(
          `INSERT INTO support.response_ratings
             (support_request_id, order_id, user_id, rating, reason_code, feedback_notes, actor)
           VALUES ($1,$2,$3,$4,$5,$6,$7)
           ON CONFLICT (support_request_id) DO UPDATE
           SET order_id = EXCLUDED.order_id,
               user_id = EXCLUDED.user_id,
               rating = EXCLUDED.rating,
               reason_code = EXCLUDED.reason_code,
               feedback_notes = EXCLUDED.feedback_notes,
               actor = EXCLUDED.actor,
               created_at = NOW()
           RETURNING rating_id, rating, reason_code, feedback_notes, actor, created_at`,
          [
            body.support_request_id,
            body.order_id,
            body.user_id ?? null,
            body.rating,
            body.reason_code ?? null,
            body.feedback_notes ?? null,
            body.actor ?? null,
          ],
        );
      } catch (error) {
        if (isPermissionDeniedError(error)) {
          res.status(403).json({
            error: "ratings_permission_denied",
            message: "App principal lacks permission on support.response_ratings. Grant table privileges in Lakebase.",
          });
          return;
        }
        throw error;
      }

      res.status(201).json(inserted.rows[0]);
    }));

    router.post("/regenerate", withJsonError(async (req, res) => {
      if (!this.pool || !this.workspaceClient) {
        res
          .status(503)
          .json({ error: "DB/API not ready", message: this.setupError });
        return;
      }
      const body = req.body as RegenerateBody;
      if (!body?.support_request_id || !body?.order_id) {
        res.status(400).json({ error: "Invalid payload" });
        return;
      }

      const prompt = createRegenerationPrompt({
        supportRequestId: body.support_request_id,
        userId: body.user_id ?? null,
        orderId: body.order_id,
        operatorContext: body.operator_context ?? "",
        currentReport: body.current_report ?? null,
      });

      let normalized: Record<string, unknown>;
      let warning: string | null = null;
      try {
        const servingResponse = await this.workspaceClient.apiClient.request({
          path: "/serving-endpoints/responses",
          method: "POST",
          headers: new Headers({
            Accept: "application/json",
            "Content-Type": "application/json",
          }),
          raw: false,
          payload: {
            model: this.supportAgentEndpoint,
            input: [{ role: "user", content: prompt }],
          },
        });
        const responseText = extractResponseText(servingResponse);
        const parsed = parseAgentReport(responseText);
        normalized = normalizeReportForUi(parsed, body.user_id ?? null);
      } catch (error) {
        if (!isServingPermissionError(error)) {
          throw error;
        }

        warning = `App service principal cannot query endpoint '${this.supportAgentEndpoint}'. Saved a fallback regeneration instead. Grant CAN_QUERY to service principal '${process.env.DATABRICKS_CLIENT_ID ?? "app service principal"}' for model-based regeneration.`;
        console.warn("[support] regenerate fallback:", warning);
        normalized = createFallbackRegeneratedReport({
          currentReport: body.current_report ?? null,
          operatorContext: body.operator_context ?? null,
          supportRequestId: body.support_request_id,
          userId: body.user_id ?? null,
          orderId: body.order_id,
        });
      }

      await this.pool.query(
        `INSERT INTO support.operator_regenerated_reports
           (support_request_id, user_id, order_id, operator_context, agent_response, actor)
         VALUES ($1,$2,$3,$4,$5::jsonb,$6)`,
        [
          body.support_request_id,
          body.user_id ?? null,
          body.order_id,
          body.operator_context ?? null,
          JSON.stringify(normalized),
          body.actor ?? null,
        ],
      );

      await this.pool.query(
        `INSERT INTO support.request_status (support_request_id, status, updated_at, last_action)
         VALUES ($1, 'in_progress', NOW(), 'regenerate_report')
         ON CONFLICT (support_request_id) DO UPDATE
         SET status = EXCLUDED.status,
             updated_at = EXCLUDED.updated_at,
             last_action = EXCLUDED.last_action`,
        [body.support_request_id],
      );

      res.status(201).json({ report: normalized, warning });
    }));
  }

  async close(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
    }
  }
}
