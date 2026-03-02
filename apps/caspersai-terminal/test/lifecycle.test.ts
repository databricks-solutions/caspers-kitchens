import test from "node:test";
import assert from "node:assert/strict";
import request from "supertest";
import { createApp } from "../src/http/app.js";
import type { AppConfig } from "../src/config.js";
import { Logger } from "../src/logging/logger.js";
import { ServiceRegistry } from "../src/services/registry.js";
import type { RuntimeService } from "../src/services/contracts.js";
import type { TerminalTypeRegistry } from "../src/terminalTypes/types.js";
import { FakeSessionManager } from "./helpers.js";

function makeConfig(overrides?: Partial<AppConfig>): AppConfig {
  return {
    appName: "databricks-app-terminal-test",
    host: "127.0.0.1",
    port: 0,
    shell: "/bin/bash",
    webRoot: `${process.cwd()}/public`,
    strictRuntimeChecks: false,
    defaultCols: 120,
    defaultRows: 30,
    maxCols: 400,
    maxRows: 200,
    maxSessions: 64,
    maxHistoryChars: 100_000,
    maxInputBytes: 16_384,
    maxWsMessageBytes: 65_536,
    wsBackpressureBytes: 1_000_000,
    diagnosticsTtlMs: 5_000,
    sessionEnvHookTimeoutMs: 50,
    sessionDefaultCwd: process.cwd(),
    terminalTypesRoot: `${process.cwd()}/.test-terminal-types`,
    toolsRoot: `${process.cwd()}/.test-tools`,
    npmGlobalPrefix: `${process.cwd()}/.test-npm-global`,
    npmCacheDir: `${process.cwd()}/.test-npm-cache`,
    databricksHost: "https://adb-test.databricks.com",
    userAccessTokenHeader: "x-forwarded-access-token",
    allowUserTokenAuth: true,
    serviceModules: [],
    logLevel: "error",
    logPath: undefined,
    ...overrides,
  };
}

function makeTerminalTypes(
  custom: Array<{ id: string; name: string; badge?: string; description?: string; entrypointPath?: string }> = [],
): TerminalTypeRegistry {
  const base = {
    id: "terminal",
    name: "Terminal",
    badge: "terminal",
    description: "Plain shell session",
    default: true,
    builtIn: true,
  };

  const all = [
    base,
    ...custom.map((type) => ({
      id: type.id,
      name: type.name,
      badge: type.badge || type.id,
      description: type.description,
      default: false,
      builtIn: false,
    })),
  ];

  const byId = new Map<string, any>();
  byId.set(base.id, base);
  for (const type of custom) {
    byId.set(type.id, {
      id: type.id,
      name: type.name,
      badge: type.badge || type.id,
      description: type.description,
      default: false,
      builtIn: false,
      entrypointPath: type.entrypointPath,
    });
  }

  return {
    listTypes: () => all.map((type) => ({ ...type })),
    resolveType: (id) => {
      const type = byId.get(id);
      return type ? { ...type } : undefined;
    },
    getDefaultType: () => ({ ...base }),
  };
}

function makeApp(
  services: RuntimeService[] = [],
  configOverride?: Partial<AppConfig>,
  terminalTypes: TerminalTypeRegistry = makeTerminalTypes(),
) {
  const config = makeConfig(configOverride);
  const logger = new Logger({
    appName: config.appName,
    level: "error",
  });

  const sessionManager = new FakeSessionManager();
  const registry = new ServiceRegistry(
    services,
    {
      config,
      logger,
    },
    logger,
    config.sessionEnvHookTimeoutMs,
  );

  const diagnostics = {
    getDiagnostics: async () => ({
      generatedAt: new Date().toISOString(),
      uptimeSec: 1,
      coreReady: true,
      requiredServicesReady: true,
      ready: true,
      coreChecks: [],
      services: {
        health: [],
        readiness: [],
      },
      sessions: sessionManager.getStats(),
    }),
  };

  return {
    app: createApp({
      config,
      logger,
      sessions: sessionManager,
      terminalTypes,
      services: registry,
      diagnostics,
    }),
    sessionManager,
    registry,
  };
}

test("session lifecycle endpoints work", async () => {
  const { app, sessionManager } = makeApp();

  const before = await request(app).get("/api/sessions").expect(200);
  assert.equal(before.body.ok, true);
  assert.equal(before.body.data.sessions.length, 0);

  const created = await request(app).post("/api/sessions").send({}).expect(201);
  assert.equal(created.body.ok, true);
  assert.equal(created.body.data.authMode, "m2m");
  assert.equal(created.body.data.typeId, "terminal");

  const sessionId = created.body.data.session.sessionId;
  assert.match(sessionId, /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i);

  const attached = await request(app)
    .post(`/api/sessions/${encodeURIComponent(sessionId)}/attach`)
    .send({})
    .expect(200);

  assert.equal(attached.body.data.sessionId, sessionId);
  assert.match(attached.body.data.websocketPath, /\/ws\/terminal\?sessionId=/);

  await request(app)
    .post(`/api/sessions/${encodeURIComponent(sessionId)}/input`)
    .send({ data: "echo hi\r" })
    .expect(200);

  await request(app)
    .post(`/api/sessions/${encodeURIComponent(sessionId)}/resize`)
    .send({ cols: 140, rows: 40 })
    .expect(200);

  const listed = await request(app).get("/api/sessions").expect(200);
  assert.equal(listed.body.data.sessions.length, 1);

  assert.equal(sessionManager.writes.length, 1);
  assert.equal(sessionManager.resizes.length >= 1, true);

  await request(app).delete(`/api/sessions/${encodeURIComponent(sessionId)}`).expect(200);

  const after = await request(app).get("/api/sessions").expect(200);
  assert.equal(after.body.data.sessions.length, 0);
});

test("session types endpoint lists built-in terminal type", async () => {
  const { app } = makeApp();

  const response = await request(app).get("/api/session-types").expect(200);

  assert.equal(response.body.ok, true);
  assert.equal(Array.isArray(response.body.data.types), true);
  assert.equal(response.body.data.types.length >= 1, true);

  const terminal = response.body.data.types.find((type: { id: string }) => type.id === "terminal");
  assert.equal(Boolean(terminal), true);
});

test("session create rejects unknown session type", async () => {
  const { app } = makeApp();

  const response = await request(app)
    .post("/api/sessions")
    .send({
      typeId: "unknown-type",
    })
    .expect(400);

  assert.equal(response.body.ok, false);
  assert.equal(response.body.error.code, "INVALID_SESSION_TYPE");
});

test("session create supports custom session type", async () => {
  const customTypes = makeTerminalTypes([
    {
      id: "claude",
      name: "Claude Code",
      entrypointPath: "/tmp/terminal-types/claude/launch.sh",
    },
  ]);

  const { app, sessionManager } = makeApp([], undefined, customTypes);

  const created = await request(app)
    .post("/api/sessions")
    .send({
      typeId: "claude",
    })
    .expect(201);

  assert.equal(created.body.ok, true);
  assert.equal(created.body.data.typeId, "claude");

  assert.equal(sessionManager.creates.length, 1);
  assert.equal(sessionManager.creates[0].typeId, "claude");
  assert.equal(sessionManager.creates[0].typeEntrypointPath, "/tmp/terminal-types/claude/launch.sh");
});

test("session create supports user auth mode for Databricks CLI env", async () => {
  const { app, sessionManager } = makeApp();

  const created = await request(app)
    .post("/api/sessions")
    .set("x-forwarded-access-token", "user.token.value")
    .send({
      authMode: "user",
    })
    .expect(201);

  assert.equal(created.body.ok, true);
  assert.equal(created.body.data.authMode, "user");

  assert.equal(sessionManager.creates.length, 1);
  const env = sessionManager.creates[0].env || {};
  assert.equal(env.DBX_APP_TERMINAL_AUTH_MODE, "user");
  assert.equal(env.DATABRICKS_HOST, "https://adb-test.databricks.com");
  assert.equal(env.DATABRICKS_TOKEN, "user.token.value");
});

test("user auth mode requires forwarded access token header", async () => {
  const { app } = makeApp();

  const response = await request(app)
    .post("/api/sessions")
    .send({
      authMode: "user",
    })
    .expect(400);

  assert.equal(response.body.ok, false);
  assert.equal(response.body.error.code, "USER_ACCESS_TOKEN_MISSING");
});

test("user auth mode requires configured Databricks host", async () => {
  const { app } = makeApp([], {
    databricksHost: undefined,
  });

  const response = await request(app)
    .post("/api/sessions")
    .set("x-forwarded-access-token", "user.token.value")
    .send({
      authMode: "user",
    })
    .expect(500);

  assert.equal(response.body.ok, false);
  assert.equal(response.body.error.code, "DATABRICKS_HOST_UNAVAILABLE");
});

test("session auth mode can toggle and uses cached user token", async () => {
  const { app, sessionManager } = makeApp();

  const created = await request(app)
    .post("/api/sessions")
    .set("x-forwarded-access-token", "cached.user.token")
    .send({
      authMode: "m2m",
    })
    .expect(201);

  const sessionId = created.body.data.session.sessionId;

  const switchedToUser = await request(app)
    .post(`/api/sessions/${encodeURIComponent(sessionId)}/auth-mode`)
    .send({
      mode: "user",
    })
    .expect(200);

  assert.equal(switchedToUser.body.ok, true);
  assert.equal(switchedToUser.body.data.authMode, "user");

  const switchedToM2m = await request(app)
    .post(`/api/sessions/${encodeURIComponent(sessionId)}/auth-mode`)
    .send({
      mode: "m2m",
    })
    .expect(200);

  assert.equal(switchedToM2m.body.ok, true);
  assert.equal(switchedToM2m.body.data.authMode, "m2m");

  assert.equal(sessionManager.creates.length, 1);
  assert.equal(sessionManager.creates[0].userAccessToken, "cached.user.token");
});

test("session auth mode endpoint returns current mode", async () => {
  const { app } = makeApp();

  const created = await request(app)
    .post("/api/sessions")
    .set("x-forwarded-access-token", "cached.user.token")
    .send({
      authMode: "m2m",
    })
    .expect(201);

  const sessionId = created.body.data.session.sessionId;

  const modeBefore = await request(app)
    .get(`/api/sessions/${encodeURIComponent(sessionId)}/auth-mode`)
    .expect(200);

  assert.equal(modeBefore.body.ok, true);
  assert.equal(modeBefore.body.data.authMode, "m2m");

  await request(app)
    .post(`/api/sessions/${encodeURIComponent(sessionId)}/auth-mode`)
    .send({
      mode: "user",
    })
    .expect(200);

  const modeAfter = await request(app)
    .get(`/api/sessions/${encodeURIComponent(sessionId)}/auth-mode`)
    .expect(200);

  assert.equal(modeAfter.body.ok, true);
  assert.equal(modeAfter.body.data.authMode, "user");
});

test("session auth mode switch to user fails without cached or forwarded token", async () => {
  const { app } = makeApp();

  const created = await request(app).post("/api/sessions").send({}).expect(201);
  const sessionId = created.body.data.session.sessionId;

  const response = await request(app)
    .post(`/api/sessions/${encodeURIComponent(sessionId)}/auth-mode`)
    .send({
      mode: "user",
    })
    .expect(400);

  assert.equal(response.body.ok, false);
  assert.equal(response.body.error.code, "USER_ACCESS_TOKEN_MISSING");
});

test("optional session env hook timeout does not block create", async () => {
  const slowService: RuntimeService = {
    name: "slow-env",
    async start() {
      // no-op
    },
    async stop() {
      // no-op
    },
    async enrichSessionEnv() {
      await new Promise((resolve) => {
        setTimeout(resolve, 500);
      });

      return {
        SHOULD_NOT_APPEAR: "1",
      };
    },
  };

  const { app, registry } = makeApp([slowService], {
    sessionEnvHookTimeoutMs: 25,
  });

  await registry.startAll();

  const startedAt = Date.now();
  const created = await request(app).post("/api/sessions").send({}).expect(201);
  const elapsedMs = Date.now() - startedAt;

  assert.equal(created.body.ok, true);
  assert.ok(elapsedMs < 250, `session creation took too long: ${elapsedMs}ms`);
});
