import fs from "node:fs";
import path from "node:path";

export type AppConfig = {
  appName: string;
  host: string;
  port: number;
  shell: string;
  webRoot: string;
  strictRuntimeChecks: boolean;
  defaultCols: number;
  defaultRows: number;
  maxCols: number;
  maxRows: number;
  maxSessions: number;
  maxHistoryChars: number;
  maxInputBytes: number;
  maxWsMessageBytes: number;
  wsBackpressureBytes: number;
  diagnosticsTtlMs: number;
  sessionEnvHookTimeoutMs: number;
  sessionDefaultCwd: string;
  terminalTypesRoot: string;
  toolsRoot: string;
  npmGlobalPrefix: string;
  npmCacheDir: string;
  databricksHost?: string;
  userAccessTokenHeader: string;
  allowUserTokenAuth: boolean;
  serviceModules: string[];
  logLevel: "debug" | "info" | "warn" | "error";
  logPath?: string;
};

function envNumber(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }

  const parsed = Number(raw);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function envBool(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (!raw) {
    return fallback;
  }

  const normalized = raw.trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  return fallback;
}

function envString(name: string): string | undefined {
  const raw = process.env[name];
  if (!raw) {
    return undefined;
  }
  const trimmed = raw.trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function envList(name: string): string[] {
  const raw = envString(name);
  if (!raw) {
    return [];
  }

  return raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
}

function directoryExists(candidate: string): boolean {
  try {
    return fs.statSync(candidate).isDirectory();
  } catch {
    return false;
  }
}

function normalizeDatabricksHost(value: string): string {
  if (value.startsWith("http://") || value.startsWith("https://")) {
    return value;
  }

  return `https://${value}`;
}

function resolveDatabricksHost(): string | undefined {
  const explicitHost = envString("DATABRICKS_HOST");
  if (explicitHost) {
    return normalizeDatabricksHost(explicitHost);
  }

  const serverHostname = envString("DATABRICKS_SERVER_HOSTNAME");
  if (serverHostname) {
    return normalizeDatabricksHost(serverHostname);
  }

  return undefined;
}

export function loadConfig(): AppConfig {
  const cwd = process.cwd();
  const defaultCols = clamp(envNumber("DEFAULT_COLS", 120), 40, 400);
  const defaultRows = clamp(envNumber("DEFAULT_ROWS", 30), 10, 200);

  const databricksRuntimeRoot = "/app/python";
  const runtimeRoot = envString("DBX_APP_TERMINAL_RUNTIME_ROOT")
    || (directoryExists(databricksRuntimeRoot) ? databricksRuntimeRoot : cwd);

  const sessionDefaultCwd = envString("SESSION_DEFAULT_CWD") || runtimeRoot;
  const terminalTypesRoot = envString("TERMINAL_TYPES_ROOT") || path.resolve(cwd, "terminal-types");
  const toolsRoot = envString("DBX_APP_TERMINAL_TOOLS_ROOT") || path.join(runtimeRoot, ".dbx-app-terminal-tools");
  const npmGlobalPrefix = envString("DBX_APP_TERMINAL_NPM_PREFIX") || path.join(toolsRoot, "npm-global");
  const npmCacheDir = envString("DBX_APP_TERMINAL_NPM_CACHE") || path.join(toolsRoot, "npm-cache");

  return {
    appName: envString("APP_NAME") || "databricks-app-terminal",
    host: envString("HOST") || "0.0.0.0",
    port: envNumber("PORT", 8080),
    shell: envString("SHELL") || "/bin/bash",
    webRoot: envString("WEB_ROOT") || path.resolve(cwd, "public"),
    strictRuntimeChecks: envBool("STRICT_RUNTIME_CHECKS", true),
    defaultCols,
    defaultRows,
    maxCols: clamp(envNumber("MAX_COLS", 400), 80, 600),
    maxRows: clamp(envNumber("MAX_ROWS", 200), 24, 400),
    maxSessions: clamp(envNumber("MAX_SESSIONS", 64), 1, 500),
    maxHistoryChars: clamp(envNumber("MAX_HISTORY_CHARS", 100_000), 1_000, 2_000_000),
    maxInputBytes: clamp(envNumber("MAX_INPUT_BYTES", 16_384), 64, 1_000_000),
    maxWsMessageBytes: clamp(envNumber("MAX_WS_MESSAGE_BYTES", 65_536), 256, 2_000_000),
    wsBackpressureBytes: clamp(envNumber("WS_BACKPRESSURE_BYTES", 1_000_000), 10_000, 10_000_000),
    diagnosticsTtlMs: clamp(envNumber("DIAGNOSTICS_TTL_MS", 5_000), 100, 60_000),
    sessionEnvHookTimeoutMs: clamp(envNumber("SESSION_ENV_HOOK_TIMEOUT_MS", 100), 10, 5_000),
    sessionDefaultCwd,
    terminalTypesRoot,
    toolsRoot,
    npmGlobalPrefix,
    npmCacheDir,
    databricksHost: resolveDatabricksHost(),
    userAccessTokenHeader: (envString("USER_ACCESS_TOKEN_HEADER") || "x-forwarded-access-token").toLowerCase(),
    allowUserTokenAuth: envBool("ALLOW_USER_TOKEN_AUTH", true),
    serviceModules: envList("SERVICE_MODULES"),
    logLevel: (envString("LOG_LEVEL") as AppConfig["logLevel"] | undefined) || "info",
    logPath: envString("LOG_PATH") || path.resolve(cwd, "logs", "databricks-app-terminal.jsonl"),
  };
}
