import test from "node:test";
import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import request from "supertest";
import WebSocket from "ws";
import { createTerminalServer } from "../src/http/app.js";
import type { AppConfig } from "../src/config.js";
import { Logger } from "../src/logging/logger.js";
import { ServiceRegistry } from "../src/services/registry.js";
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

function waitForMessage(
  socket: WebSocket,
  predicate: (message: any) => boolean,
  timeoutMs = 2_000,
): Promise<any> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error("Timed out waiting for websocket message"));
    }, timeoutMs);

    const onMessage = (raw: WebSocket.RawData) => {
      try {
        const parsed = JSON.parse(raw.toString());
        if (predicate(parsed)) {
          cleanup();
          resolve(parsed);
        }
      } catch {
        // ignore non-json payload
      }
    };

    const onError = (error: Error) => {
      cleanup();
      reject(error);
    };

    const cleanup = () => {
      clearTimeout(timeout);
      socket.off("message", onMessage);
      socket.off("error", onError);
    };

    socket.on("message", onMessage);
    socket.on("error", onError);
  });
}

test("websocket attach path streams output", async (t) => {
  const config = makeConfig();
  const logger = new Logger({
    appName: config.appName,
    level: "error",
  });

  const sessions = new FakeSessionManager();
  const registry = new ServiceRegistry(
    [],
    {
      config,
      logger,
    },
    logger,
    config.sessionEnvHookTimeoutMs,
  );

  await registry.startAll();

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
      sessions: sessions.getStats(),
    }),
  };

  const terminalTypes = {
    listTypes: () => [
      {
        id: "terminal",
        name: "Terminal",
        badge: "terminal",
        description: "Plain shell session",
        default: true,
        builtIn: true,
      },
    ],
    resolveType: (id: string) => {
      if (id !== "terminal") {
        return undefined;
      }
      return {
        id: "terminal",
        name: "Terminal",
        badge: "terminal",
        description: "Plain shell session",
        default: true,
        builtIn: true,
      };
    },
    getDefaultType: () => ({
      id: "terminal",
      name: "Terminal",
      badge: "terminal",
      description: "Plain shell session",
      default: true,
      builtIn: true,
    }),
  };

  const { server } = createTerminalServer({
    config,
    logger,
    sessions,
    terminalTypes,
    services: registry,
    diagnostics,
  });

  await new Promise<void>((resolve) => {
    server.listen(0, "127.0.0.1", () => resolve());
  });

  t.after(async () => {
    await sessions.shutdown();
    await registry.stopAll();
    await new Promise<void>((resolve, reject) => {
      server.close((error) => {
        if (error) {
          reject(error);
          return;
        }
        resolve();
      });
    });
  });

  const address = server.address() as AddressInfo;

  const created = await request(server).post("/api/sessions").send({}).expect(201);
  const sessionId = created.body.data.session.sessionId;

  const attached = await request(server)
    .post(`/api/sessions/${encodeURIComponent(sessionId)}/attach`)
    .send({})
    .expect(200);

  const ws = new WebSocket(`ws://127.0.0.1:${address.port}${attached.body.data.websocketPath}`);

  const ready = await waitForMessage(ws, (message) => message.type === "ready");
  assert.equal(ready.sessionId, sessionId);

  ws.send(
    JSON.stringify({
      type: "input",
      data: "hello",
    }),
  );

  const output = await waitForMessage(ws, (message) => message.type === "output");
  assert.equal(output.data, "hello");

  ws.send(
    JSON.stringify({
      type: "resize",
      cols: 140,
      rows: 40,
    }),
  );

  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(sessions.resizes.some((resize) => resize.sessionId === sessionId), true);

  ws.close();
});
