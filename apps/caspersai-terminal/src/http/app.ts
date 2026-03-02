import http from "node:http";
import path from "node:path";
import { randomUUID } from "node:crypto";
import express, { type NextFunction, type Request, type Response } from "express";
import { z } from "zod";
import { AppError, fail, ok, toApiFailure } from "../api/types.js";
import { parseBody, parseParams, parseQuery } from "../api/validation.js";
import type { AppConfig } from "../config.js";
import type { Logger } from "../logging/logger.js";
import { SESSION_ID_PATTERN } from "../sessions/ptySessionManager.js";
import type { SessionAuthMode, SessionManager } from "../sessions/types.js";
import type { RuntimeDiagnosticsManager } from "../runtime/diagnostics.js";
import type { ServiceRegistry } from "../services/registry.js";
import type { TerminalTypeRegistry } from "../terminalTypes/types.js";
import { TerminalGateway } from "../ws/terminalGateway.js";
import { v7 as uuidv7 } from "uuid";

const sessionParamsSchema = z.object({
  sessionId: z.string().regex(SESSION_ID_PATTERN),
});

const createSessionBodySchema = z
  .object({
    cwd: z.string().min(1).optional(),
    cols: z.number().int().positive().optional(),
    rows: z.number().int().positive().optional(),
    authMode: z.enum(["m2m", "user", "user-token"]).optional(),
    typeId: z.string().regex(/^[a-z0-9][a-z0-9-_]{0,63}$/).optional(),
  })
  .default({});

const setAuthModeBodySchema = z.object({
  mode: z.enum(["m2m", "user", "user-token"]),
});

const writeInputBodySchema = z.object({
  data: z.string().min(1),
});

const resizeBodySchema = z.object({
  cols: z.number().int().positive(),
  rows: z.number().int().positive(),
});

const diagnosticsQuerySchema = z.object({
  refresh: z.enum(["true", "false"]).optional(),
});

type SessionEnvBuilder = Pick<ServiceRegistry, "buildSessionEnv">;
type DiagnosticsProvider = Pick<RuntimeDiagnosticsManager, "getDiagnostics">;

export type AppServices = {
  config: AppConfig;
  logger: Logger;
  sessions: SessionManager;
  terminalTypes: TerminalTypeRegistry;
  services: SessionEnvBuilder;
  diagnostics: DiagnosticsProvider;
};

function withErrorBoundary(
  handler: (req: Request, res: Response) => Promise<void> | void,
): (req: Request, res: Response, next: NextFunction) => void {
  return (req, res, next) => {
    Promise.resolve(handler(req, res)).catch(next);
  };
}

function requestId(req: Request): string {
  const id = req.header("x-request-id");
  return id && id.length > 0 ? id : randomUUID();
}

function requestActor(req: Request): string {
  return req.header("x-user-email") || req.ip || "unknown";
}

type RequestedAuthMode = "m2m" | "user" | "user-token";

type SessionAuthResolutionInput = {
  requestedMode: RequestedAuthMode | undefined;
  userAccessToken?: string;
  config: AppConfig;
};

type SessionAuthResolution = {
  mode: SessionAuthMode;
  env: Record<string, string>;
  cachedUserAccessToken?: string;
};

function readHeaderValue(req: Request, name: string): string | undefined {
  const raw = req.header(name);
  if (!raw) {
    return undefined;
  }

  const value = raw.trim();
  return value.length > 0 ? value : undefined;
}

function normalizeAuthMode(mode: RequestedAuthMode | undefined): SessionAuthMode {
  if (mode === "user" || mode === "user-token") {
    return "user";
  }

  return "m2m";
}

function assertUserTokenAuthEnabled(config: AppConfig): void {
  if (!config.allowUserTokenAuth) {
    throw new AppError(403, "USER_TOKEN_AUTH_DISABLED", "User-token session mode is disabled", false);
  }
}

function resolveSessionAuth(input: SessionAuthResolutionInput): SessionAuthResolution {
  const mode = normalizeAuthMode(input.requestedMode);

  if (mode === "m2m") {
    return {
      mode,
      cachedUserAccessToken: input.userAccessToken,
      env: {
        DBX_APP_TERMINAL_AUTH_MODE: "m2m",
        DBX_APP_TERMINAL_USER_TOKEN_HEADER: input.config.userAccessTokenHeader,
      },
    };
  }

  assertUserTokenAuthEnabled(input.config);

  if (!input.userAccessToken) {
    throw new AppError(
      400,
      "USER_ACCESS_TOKEN_MISSING",
      "User access token header is required for authMode=user",
      false,
      {
        header: input.config.userAccessTokenHeader,
      },
    );
  }

  if (!input.config.databricksHost) {
    throw new AppError(
      500,
      "DATABRICKS_HOST_UNAVAILABLE",
      "Databricks host is not configured for user auth mode",
      true,
      {
        expectedEnv: ["DATABRICKS_HOST", "DATABRICKS_SERVER_HOSTNAME"],
      },
    );
  }

  return {
    mode,
    cachedUserAccessToken: input.userAccessToken,
    env: {
      DBX_APP_TERMINAL_AUTH_MODE: "user",
      DBX_APP_TERMINAL_USER_TOKEN_HEADER: input.config.userAccessTokenHeader,
      DATABRICKS_HOST: input.config.databricksHost,
      DATABRICKS_TOKEN: input.userAccessToken,
    },
  };
}

export function createApp(services: AppServices): express.Express {
  const app = express();

  app.use(express.json({ limit: "128kb" }));

  app.use((req, res, next) => {
    const start = Date.now();
    const id = requestId(req);
    res.locals.requestId = id;
    res.setHeader("x-request-id", id);

    res.on("finish", () => {
      services.logger.info("http.request", {
        requestId: id,
        method: req.method,
        path: req.path,
        status: res.statusCode,
        durationMs: Date.now() - start,
      });
    });

    next();
  });

  app.get(
    "/health",
    withErrorBoundary(async (_req, res) => {
      res.status(200).json(
        ok({
          name: services.config.appName,
          status: "ok",
          uptimeSec: Math.round(process.uptime()),
          version: process.env.npm_package_version || "0.1.0",
        }),
      );
    }),
  );

  app.get(
    "/ready",
    withErrorBoundary(async (_req, res) => {
      const diagnostics = await services.diagnostics.getDiagnostics(false);

      if (!diagnostics.ready) {
        res.status(503).json(
          fail(
            "NOT_READY",
            "Runtime is not ready",
            res.locals.requestId,
            true,
            {
              coreReady: diagnostics.coreReady,
              requiredServicesReady: diagnostics.requiredServicesReady,
            },
          ),
        );
        return;
      }

      res.status(200).json(
        ok({
          ready: true,
          coreReady: diagnostics.coreReady,
          requiredServicesReady: diagnostics.requiredServicesReady,
        }),
      );
    }),
  );

  app.get(
    "/api/runtime/diagnostics",
    withErrorBoundary(async (req, res) => {
      const query = parseQuery(req, diagnosticsQuerySchema);
      const diagnostics = await services.diagnostics.getDiagnostics(query.refresh === "true");

      res.status(200).json(ok(diagnostics));
    }),
  );

  app.get(
    "/api/session-types",
    withErrorBoundary(async (_req, res) => {
      const types = services.terminalTypes.listTypes();
      res.status(200).json(
        ok({
          types,
        }),
      );
    }),
  );

  app.get(
    "/api/sessions",
    withErrorBoundary(async (_req, res) => {
      const list = await services.sessions.listSessions();
      res.status(200).json(
        ok({
          sessions: list,
        }),
      );
    }),
  );

  app.post(
    "/api/sessions",
    withErrorBoundary(async (req, res) => {
      const payload = parseBody(req, createSessionBodySchema);
      const sessionId = uuidv7();
      const actor = requestActor(req);
      const sessionCwd = payload.cwd || services.config.sessionDefaultCwd;

      const requestedTypeId = payload.typeId || services.terminalTypes.getDefaultType().id;
      const sessionType = services.terminalTypes.resolveType(requestedTypeId);

      if (!sessionType) {
        throw new AppError(400, "INVALID_SESSION_TYPE", "Unknown session type", false, {
          typeId: requestedTypeId,
          availableTypes: services.terminalTypes.listTypes().map((type) => type.id),
        });
      }

      const userAccessToken = readHeaderValue(req, services.config.userAccessTokenHeader);
      const auth = resolveSessionAuth({
        requestedMode: payload.authMode,
        userAccessToken,
        config: services.config,
      });

      const env = await services.services.buildSessionEnv({
        sessionId,
        actor,
        cwd: sessionCwd,
        typeId: sessionType.id,
      });

      const session = await services.sessions.createSession({
        sessionId,
        cwd: sessionCwd,
        cols: payload.cols,
        rows: payload.rows,
        authMode: auth.mode,
        typeId: sessionType.id,
        typeEntrypointPath: sessionType.entrypointPath,
        userAccessToken: auth.cachedUserAccessToken,
        databricksHost: services.config.databricksHost,
        userAccessTokenHeader: services.config.userAccessTokenHeader,
        env: {
          DBX_APP_TERMINAL_SESSION_ID: sessionId,
          DBX_APP_TERMINAL_ACTOR: actor,
          DBX_APP_TERMINAL_TYPE_ID: sessionType.id,
          ...env.env,
          ...auth.env,
        },
      });

      services.logger.info("api.session.create", {
        requestId: res.locals.requestId,
        sessionId,
        actor,
        cwd: sessionCwd,
        authMode: session.authMode,
        typeId: session.typeId,
        warnings: env.warnings,
      });

      res.status(201).json(
        ok({
          session,
          authMode: session.authMode,
          typeId: session.typeId,
          websocketPath: `/ws/terminal?sessionId=${encodeURIComponent(session.sessionId)}`,
        }),
      );
    }),
  );

  app.post(
    "/api/sessions/:sessionId/attach",
    withErrorBoundary(async (req, res) => {
      const params = parseParams(req, sessionParamsSchema);
      await services.sessions.ensureSessionExists(params.sessionId);

      res.status(200).json(
        ok({
          sessionId: params.sessionId,
          websocketPath: `/ws/terminal?sessionId=${encodeURIComponent(params.sessionId)}`,
        }),
      );
    }),
  );

  app.post(
    "/api/sessions/:sessionId/input",
    withErrorBoundary(async (req, res) => {
      const params = parseParams(req, sessionParamsSchema);
      const payload = parseBody(req, writeInputBodySchema);

      await services.sessions.writeInput(params.sessionId, payload.data);

      res.status(200).json(
        ok({
          sessionId: params.sessionId,
          accepted: true,
        }),
      );
    }),
  );

  app.post(
    "/api/sessions/:sessionId/resize",
    withErrorBoundary(async (req, res) => {
      const params = parseParams(req, sessionParamsSchema);
      const payload = parseBody(req, resizeBodySchema);

      await services.sessions.resizeSession(params.sessionId, payload.cols, payload.rows);

      res.status(200).json(
        ok({
          sessionId: params.sessionId,
          cols: payload.cols,
          rows: payload.rows,
        }),
      );
    }),
  );

  app.get(
    "/api/sessions/:sessionId/auth-mode",
    withErrorBoundary(async (req, res) => {
      const params = parseParams(req, sessionParamsSchema);
      const session = await services.sessions.getSessionInfo(params.sessionId);

      res.status(200).json(
        ok({
          sessionId: session.sessionId,
          authMode: session.authMode,
        }),
      );
    }),
  );

  app.post(
    "/api/sessions/:sessionId/auth-mode",
    withErrorBoundary(async (req, res) => {
      const params = parseParams(req, sessionParamsSchema);
      const payload = parseBody(req, setAuthModeBodySchema);
      const mode = normalizeAuthMode(payload.mode);

      if (mode === "user") {
        assertUserTokenAuthEnabled(services.config);
      }

      const userAccessToken = readHeaderValue(req, services.config.userAccessTokenHeader);

      const session = await services.sessions.setSessionAuthMode(params.sessionId, {
        mode,
        userAccessToken,
        databricksHost: services.config.databricksHost,
        userAccessTokenHeader: services.config.userAccessTokenHeader,
      });

      services.logger.info("api.session.auth_mode", {
        requestId: res.locals.requestId,
        sessionId: params.sessionId,
        authMode: session.authMode,
      });

      res.status(200).json(
        ok({
          session,
          authMode: session.authMode,
        }),
      );
    }),
  );

  app.delete(
    "/api/sessions/:sessionId",
    withErrorBoundary(async (req, res) => {
      const params = parseParams(req, sessionParamsSchema);
      await services.sessions.killSession(params.sessionId);

      res.status(200).json(
        ok({
          sessionId: params.sessionId,
          killed: true,
        }),
      );
    }),
  );

  app.use("/vendor/xterm", express.static(path.resolve(process.cwd(), "node_modules/xterm")));
  app.use(
    "/vendor/xterm-addon-fit",
    express.static(path.resolve(process.cwd(), "node_modules/@xterm/addon-fit")),
  );
  app.use(express.static(services.config.webRoot));

  app.get("/", (_req, res) => {
    res.sendFile(path.resolve(services.config.webRoot, "index.html"));
  });

  app.use((req, res, next) => {
    if (req.path.startsWith("/api/") || req.path === "/health" || req.path === "/ready") {
      const id = res.locals.requestId as string;
      res.status(404).json(
        fail("ROUTE_NOT_FOUND", `Route not found: ${req.method} ${req.path}`, id, false, {
          path: req.path,
          method: req.method,
        }),
      );
      return;
    }

    next();
  });

  app.use((error: unknown, req: Request, res: Response, _next: NextFunction) => {
    const id = (res.locals.requestId as string) || randomUUID();
    const { status, body } = toApiFailure(error, id);

    services.logger.error("api.error", {
      requestId: id,
      path: req.path,
      method: req.method,
      status,
      code: body.error.code,
      message: body.error.message,
    });

    res.status(status).json(body);
  });

  return app;
}

export function createTerminalServer(services: AppServices): {
  app: express.Express;
  server: http.Server;
  terminalGateway: TerminalGateway;
} {
  const app = createApp(services);
  const server = http.createServer(app);

  const terminalGateway = new TerminalGateway(server, services.config, services.logger, services.sessions);

  return {
    app,
    server,
    terminalGateway,
  };
}
