import { Plugin } from "@databricks/appkit";
import type { IAppRouter } from "@databricks/appkit";
import { WorkspaceClient } from "@databricks/sdk-experimental";
import type { Pool } from "pg";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { createLakebasePool } from "./lib/lakebase.js";

interface ChatBody {
  session_id: string;
  message: string;
  user_agent?: string | null;
}

function extractResponseText(response: unknown): string {
  if (!response || typeof response !== "object") {
    throw new Error("Invalid serving response");
  }
  const output = (response as { output?: unknown }).output;
  if (!Array.isArray(output)) {
    throw new Error("Serving response missing output");
  }
  // The MAS output array contains intermediate routing messages followed by the
  // final answer — iterate all and keep the last non-empty text.
  let lastText = "";
  for (const message of output) {
    if (!message || typeof message !== "object") continue;
    const content = (message as { content?: unknown }).content;
    if (!Array.isArray(content)) continue;
    for (const item of content) {
      if (!item || typeof item !== "object") continue;
      const text = (item as { text?: unknown }).text;
      if (typeof text === "string" && text.trim().length > 0) {
        lastText = text;
      }
    }
  }
  if (lastText) return lastText;
  throw new Error("Serving response text not found");
}

function extractErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error ?? "unknown_error");
}

const ALLOWED_PDF_TYPES = new Set(["invoices", "contracts"]);

export class InvoicePlugin extends Plugin {
  public name = "invoice";
  protected envVars: string[] = [];
  private pool: Pool | null = null;
  private setupError: string | null = null;
  private workspaceClient: WorkspaceClient | null = null;
  private supervisorEndpoint =
    process.env.INVOICE_SUPERVISOR_ENDPOINT_NAME ?? "invoice-supervisor";
  private catalog = process.env.CATALOG ?? "";

  static manifest = {
    name: "invoice",
    displayName: "Invoice Plugin",
    description: "Invoice supervisor chat backend backed by Lakebase",
    resources: {
      required: [],
      optional: [],
    },
  };

  async setup(): Promise<void> {
    try {
      this.pool = await createLakebasePool();

      const identity = await this.pool.query<{ current_user: string }>("SELECT current_user");
      console.log(
        "[invoice] lakebase current_user:",
        identity.rows[0]?.current_user ?? "unknown",
      );

      this.workspaceClient = new WorkspaceClient({
        host: process.env.DATABRICKS_HOST,
        ...(process.env.DATABRICKS_CONFIG_PROFILE && {
          profile: process.env.DATABRICKS_CONFIG_PROFILE,
        }),
      });

      const executeSetupDdl = async (sql: string): Promise<void> => {
        try {
          await this.pool?.query(sql);
        } catch (error) {
          const msg = extractErrorMessage(error).toLowerCase();
          if (msg.includes("permission denied") || msg.includes("must be owner")) {
            console.warn("[invoice] skipping startup DDL:", extractErrorMessage(error));
            return;
          }
          throw error;
        }
      };

      await executeSetupDdl(`CREATE SCHEMA IF NOT EXISTS procurement`);

      await executeSetupDdl(`
        CREATE TABLE IF NOT EXISTS procurement.sessions (
          session_id TEXT PRIMARY KEY,
          started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
          user_agent TEXT
        )
      `);

      await executeSetupDdl(`
        CREATE TABLE IF NOT EXISTS procurement.conversations (
          turn_id BIGSERIAL PRIMARY KEY,
          session_id TEXT NOT NULL,
          turn_index INT NOT NULL DEFAULT 0,
          user_message TEXT NOT NULL,
          agent_response TEXT NOT NULL,
          routed_to TEXT,
          latency_ms INT,
          input_tokens INT,
          output_tokens INT,
          created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
      `);

      await executeSetupDdl(`
        CREATE INDEX IF NOT EXISTS idx_conversations_session_id
        ON procurement.conversations (session_id, turn_index)
      `);
    } catch (error) {
      this.setupError = error instanceof Error ? error.message : String(error);
      console.error("[invoice] setup failed:", this.setupError);
    }
  }

  injectRoutes(router: IAppRouter): void {
    const withJsonError =
      (handler: (req: any, res: any) => Promise<void>) =>
      async (req: any, res: any) => {
        try {
          await handler(req, res);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          console.error("[invoice] route error:", message);
          if (!res.headersSent) {
            res.status(500).json({ error: "internal_error", message });
          }
        }
      };

    router.get(
      "/manifest",
      withJsonError(async (_req, res) => {
        // process.cwd() = app root (apps/invoice-terminal/) at runtime
        const manifestPath = join(process.cwd(), "server", "manifest.json");
        try {
          const raw = await readFile(manifestPath, "utf-8");
          res.setHeader("Content-Type", "application/json");
          res.setHeader("Cache-Control", "public, max-age=3600");
          res.send(raw);
        } catch {
          res.status(503).json({ error: "Manifest not available" });
        }
      }),
    );

    router.get(
      "/healthz",
      withJsonError(async (_req, res) => {
        if (!this.pool) {
          res.status(503).json({ ok: false, error: "DB not ready", message: this.setupError });
          return;
        }
        await this.pool.query("SELECT 1");
        res.json({ ok: true, endpoint: this.supervisorEndpoint });
      }),
    );

    router.post(
      "/chat",
      withJsonError(async (req, res) => {
        if (!this.pool || !this.workspaceClient) {
          res
            .status(503)
            .json({ error: "Not ready", message: this.setupError ?? "Workspace client missing" });
          return;
        }

        const body = req.body as ChatBody;
        if (!body?.session_id || !body?.message) {
          res.status(400).json({ error: "Missing required fields: session_id, message" });
          return;
        }

        // Ensure session record exists
        await this.pool.query(
          `INSERT INTO procurement.sessions (session_id, user_agent)
           VALUES ($1, $2)
           ON CONFLICT (session_id) DO NOTHING`,
          [body.session_id, body.user_agent ?? null],
        );

        // Get next turn index for this session
        const turnResult = await this.pool.query<{ next_turn: string }>(
          `SELECT COALESCE(MAX(turn_index) + 1, 0)::text AS next_turn
           FROM procurement.conversations
           WHERE session_id = $1`,
          [body.session_id],
        );
        const turn_index = Number(turnResult.rows[0]?.next_turn ?? 0);

        // Call invoice supervisor
        const start = Date.now();
        let agentResponse: string;
        let inputTokens: number | null = null;
        let outputTokens: number | null = null;

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
              model: this.supervisorEndpoint,
              input: [{ role: "user", content: body.message }],
            },
          });

          agentResponse = extractResponseText(servingResponse);

          const usage = (servingResponse as Record<string, unknown>).usage as
            | Record<string, unknown>
            | undefined;
          inputTokens =
            typeof usage?.input_tokens === "number" ? usage.input_tokens : null;
          outputTokens =
            typeof usage?.output_tokens === "number" ? usage.output_tokens : null;
        } catch (error) {
          const message = extractErrorMessage(error);
          console.error("[invoice] supervisor call failed:", message);
          const isPermError =
            message.toLowerCase().includes("do not have permission") ||
            message.toLowerCase().includes("not authorized") ||
            message.toLowerCase().includes("unauthorized");
          agentResponse = isPermError
            ? `The app service principal does not have permission to query the invoice supervisor endpoint '${this.supervisorEndpoint}'. Grant CAN_QUERY to the app's service principal.`
            : `I encountered an error reaching the invoice supervisor: ${message}`;
        }

        const latency_ms = Date.now() - start;

        // Log to Lakebase
        await this.pool.query(
          `INSERT INTO procurement.conversations
             (session_id, turn_index, user_message, agent_response, latency_ms, input_tokens, output_tokens)
           VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [
            body.session_id,
            turn_index,
            body.message,
            agentResponse,
            latency_ms,
            inputTokens,
            outputTokens,
          ],
        );

        res.json({ response: agentResponse, turn_index, latency_ms });
      }),
    );

    router.get(
      "/pdf",
      withJsonError(async (req, res) => {
        if (!this.workspaceClient) {
          res.status(503).json({ error: "Not ready" });
          return;
        }
        if (!this.catalog) {
          res.status(503).json({ error: "CATALOG env var not set" });
          return;
        }

        const type = req.query.type as string | undefined;
        const file = req.query.file as string | undefined;

        if (!type || !ALLOWED_PDF_TYPES.has(type)) {
          res.status(400).json({ error: "Invalid or missing ?type= (invoices|contracts)" });
          return;
        }
        if (!file) {
          res.status(400).json({ error: "Missing ?file=" });
          return;
        }
        // Allow only safe filename characters
        const safeFile = file.replace(/[^a-zA-Z0-9._-]/g, "");
        if (safeFile !== file || !safeFile.endsWith(".pdf")) {
          res.status(400).json({ error: "Invalid filename" });
          return;
        }

        const fsPath = `/Volumes/${this.catalog}/procurement/${type}/${safeFile}`;
        try {
          // The SDK's raw:true path doesn't handle binary responses reliably.
          // Use native fetch. authenticate() expects a Headers object (calls .set internally).
          const authHeaders = new Headers({ Accept: "application/octet-stream" });
          const cfg =
            (this.workspaceClient as unknown as Record<string, unknown>).config ??
            (this.workspaceClient as unknown as Record<string, unknown>)._cfg;
          if (cfg && typeof (cfg as Record<string, unknown>).authenticate === "function") {
            await (cfg as { authenticate: (h: Headers) => Promise<void> }).authenticate(authHeaders);
          } else if (process.env.DATABRICKS_TOKEN) {
            authHeaders.set("Authorization", `Bearer ${process.env.DATABRICKS_TOKEN}`);
          }

          const rawHost = (process.env.DATABRICKS_HOST ?? "").replace(/\/$/, "");
          const host = rawHost.startsWith("http") ? rawHost : `https://${rawHost}`;
          const fileRes = await fetch(`${host}/api/2.0/fs/files${fsPath}`, {
            headers: authHeaders,
          });

          if (!fileRes.ok) {
            const detail = await fileRes.text().catch(() => "");
            res.status(fileRes.status).json({ error: `File not found: ${safeFile}`, detail });
            return;
          }

          const buffer = Buffer.from(await fileRes.arrayBuffer());
          res.setHeader("Content-Type", "application/pdf");
          res.setHeader("Cache-Control", "private, max-age=300");
          res.send(buffer);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          console.error("[invoice] pdf fetch failed:", message);
          res.status(500).json({ error: "Failed to fetch PDF", message });
        }
      }),
    );

    router.get(
      "/sessions",
      withJsonError(async (_req, res) => {
        if (!this.pool) {
          res.status(503).json({ error: "Not ready", message: this.setupError });
          return;
        }
        const result = await this.pool.query(`
          SELECT
            s.session_id,
            s.started_at,
            COUNT(c.turn_id)::int            AS turn_count,
            MIN(c.user_message)              AS first_question,
            MAX(c.created_at)               AS last_activity
          FROM procurement.sessions s
          LEFT JOIN procurement.conversations c ON s.session_id = c.session_id
          GROUP BY s.session_id, s.started_at
          ORDER BY MAX(c.created_at) DESC NULLS LAST, s.started_at DESC
          LIMIT 30
        `);
        res.json({ sessions: result.rows });
      }),
    );

    router.get(
      "/conversations",
      withJsonError(async (req, res) => {
        if (!this.pool) {
          res.status(503).json({ error: "Not ready", message: this.setupError });
          return;
        }

        const session_id = req.query.session_id as string | undefined;
        if (!session_id) {
          res.status(400).json({ error: "Missing required query parameter: session_id" });
          return;
        }

        const result = await this.pool.query(
          `SELECT turn_id, turn_index, user_message, agent_response, latency_ms, created_at
           FROM procurement.conversations
           WHERE session_id = $1
           ORDER BY turn_index ASC`,
          [session_id],
        );

        res.json({ turns: result.rows, session_id });
      }),
    );
  }

  async close(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
    }
  }
}
