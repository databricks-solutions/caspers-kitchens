import { randomUUID } from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";
import pty, { type IPty } from "node-pty";
import { AppError } from "../api/types.js";
import type { Logger } from "../logging/logger.js";
import type {
  AttachHandlers,
  CreateSessionInput,
  SessionAuthMode,
  SessionExit,
  SessionInfo,
  SessionManager,
  SetSessionAuthModeInput,
} from "./types.js";

export const SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

type SessionClient = {
  id: string;
  onData: (data: string) => void;
  onExit: (event: SessionExit) => void;
  onAuthMode?: (mode: SessionAuthMode) => void;
};

type SessionAuthState = {
  mode: SessionAuthMode;
  userAccessToken?: string;
  databricksHost?: string;
  userAccessTokenHeader?: string;
  originalDatabricksHost?: string;
  originalDatabricksClientId?: string;
  originalDatabricksClientSecret?: string;
  stateFilePath: string;
};

type SessionRecord = {
  info: SessionInfo;
  pty: IPty;
  clients: Map<string, SessionClient>;
  history: string;
  auth: SessionAuthState;
};

export type InMemoryPtySessionManagerOptions = {
  shell: string;
  baseEnv?: Record<string, string | undefined>;
  maxSessions: number;
  maxHistoryChars: number;
  maxInputBytes: number;
  defaultCols: number;
  defaultRows: number;
  maxCols: number;
  maxRows: number;
  authStateDir: string;
  bashRcFilePath?: string;
  logger: Logger;
};

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function cleanEnv(overrides: Record<string, string | undefined>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [key, value] of Object.entries(overrides)) {
    if (typeof value === "string") {
      out[key] = value;
    }
  }
  return out;
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}

function isBashShell(shellPath: string): boolean {
  return path.basename(shellPath) === "bash";
}

function renderAuthState(state: SessionAuthState): string {
  const lines = [
    `DBX_APP_TERMINAL_STATE_MODE=${shellQuote(state.mode)}`,
    `DBX_APP_TERMINAL_STATE_DATABRICKS_HOST=${shellQuote(state.databricksHost || "")}`,
    `DBX_APP_TERMINAL_STATE_DATABRICKS_TOKEN=${shellQuote(state.userAccessToken || "")}`,
    `DBX_APP_TERMINAL_STATE_USER_TOKEN_HEADER=${shellQuote(state.userAccessTokenHeader || "x-forwarded-access-token")}`,
    `DBX_APP_TERMINAL_STATE_ORIG_DATABRICKS_HOST_SET=${shellQuote(state.originalDatabricksHost === undefined ? "0" : "1")}`,
    `DBX_APP_TERMINAL_STATE_ORIG_DATABRICKS_HOST=${shellQuote(state.originalDatabricksHost || "")}`,
    `DBX_APP_TERMINAL_STATE_ORIG_DATABRICKS_CLIENT_ID_SET=${shellQuote(state.originalDatabricksClientId === undefined ? "0" : "1")}`,
    `DBX_APP_TERMINAL_STATE_ORIG_DATABRICKS_CLIENT_ID=${shellQuote(state.originalDatabricksClientId || "")}`,
    `DBX_APP_TERMINAL_STATE_ORIG_DATABRICKS_CLIENT_SECRET_SET=${shellQuote(state.originalDatabricksClientSecret === undefined ? "0" : "1")}`,
    `DBX_APP_TERMINAL_STATE_ORIG_DATABRICKS_CLIENT_SECRET=${shellQuote(state.originalDatabricksClientSecret || "")}`,
  ];

  return `${lines.join("\n")}\n`;
}

function applyAuthModeToEnv(env: Record<string, string>, state: SessionAuthState): void {
  env.DBX_APP_TERMINAL_AUTH_MODE = state.mode;
  env.DBX_APP_TERMINAL_USER_TOKEN_HEADER = state.userAccessTokenHeader || "x-forwarded-access-token";

  if (state.mode === "user") {
    env.DATABRICKS_AUTH_TYPE = "pat";

    if (state.databricksHost) {
      env.DATABRICKS_HOST = state.databricksHost;
    } else {
      delete env.DATABRICKS_HOST;
    }

    if (state.userAccessToken) {
      env.DATABRICKS_TOKEN = state.userAccessToken;
    } else {
      delete env.DATABRICKS_TOKEN;
    }

    delete env.DATABRICKS_CLIENT_ID;
    delete env.DATABRICKS_CLIENT_SECRET;
    return;
  }

  env.DATABRICKS_AUTH_TYPE = "oauth-m2m";
  delete env.DATABRICKS_TOKEN;

  if (state.originalDatabricksHost !== undefined) {
    env.DATABRICKS_HOST = state.originalDatabricksHost;
  } else {
    delete env.DATABRICKS_HOST;
  }

  if (state.originalDatabricksClientId !== undefined) {
    env.DATABRICKS_CLIENT_ID = state.originalDatabricksClientId;
  } else {
    delete env.DATABRICKS_CLIENT_ID;
  }

  if (state.originalDatabricksClientSecret !== undefined) {
    env.DATABRICKS_CLIENT_SECRET = state.originalDatabricksClientSecret;
  } else {
    delete env.DATABRICKS_CLIENT_SECRET;
  }
}

async function writeAuthStateFile(filePath: string, content: string): Promise<void> {
  await fs.mkdir(path.dirname(filePath), { recursive: true });
  const tempPath = `${filePath}.tmp-${randomUUID()}`;

  await fs.writeFile(tempPath, content, {
    mode: 0o600,
  });

  await fs.rename(tempPath, filePath);

  try {
    await fs.chmod(filePath, 0o600);
  } catch {
    // noop
  }
}

export function assertValidSessionId(sessionId: string): void {
  if (!SESSION_ID_PATTERN.test(sessionId)) {
    throw new AppError(400, "INVALID_SESSION_ID", "Session ID must be a UUIDv7", false, {
      sessionId,
    });
  }
}

export class InMemoryPtySessionManager implements SessionManager {
  private readonly sessions = new Map<string, SessionRecord>();

  private readonly shell: string;

  private readonly baseEnv: Record<string, string | undefined>;

  private readonly maxSessions: number;

  private readonly maxHistoryChars: number;

  private readonly maxInputBytes: number;

  private readonly defaultCols: number;

  private readonly defaultRows: number;

  private readonly maxCols: number;

  private readonly maxRows: number;

  private readonly authStateDir: string;

  private readonly bashRcFilePath?: string;

  private readonly logger: Logger;

  constructor(options: InMemoryPtySessionManagerOptions) {
    this.shell = options.shell;
    this.baseEnv = options.baseEnv || {};
    this.maxSessions = options.maxSessions;
    this.maxHistoryChars = options.maxHistoryChars;
    this.maxInputBytes = options.maxInputBytes;
    this.defaultCols = options.defaultCols;
    this.defaultRows = options.defaultRows;
    this.maxCols = options.maxCols;
    this.maxRows = options.maxRows;
    this.authStateDir = options.authStateDir;
    this.bashRcFilePath = options.bashRcFilePath;
    this.logger = options.logger;
  }

  async listSessions(): Promise<SessionInfo[]> {
    return [...this.sessions.values()]
      .map((session) => this.toSessionInfo(session))
      .sort((a, b) => a.createdAt - b.createdAt);
  }

  async createSession(input: CreateSessionInput): Promise<SessionInfo> {
    assertValidSessionId(input.sessionId);

    if (this.sessions.has(input.sessionId)) {
      throw new AppError(409, "SESSION_EXISTS", "Session already exists", false, {
        sessionId: input.sessionId,
      });
    }

    if (this.sessions.size >= this.maxSessions) {
      throw new AppError(
        429,
        "SESSION_LIMIT_REACHED",
        `Session limit reached (${this.maxSessions})`,
        true,
        {
          maxSessions: this.maxSessions,
        },
      );
    }

    const cols = clamp(input.cols || this.defaultCols, 20, this.maxCols);
    const rows = clamp(input.rows || this.defaultRows, 10, this.maxRows);
    const cwd = input.cwd || process.cwd();

    const mergedEnv: Record<string, string> = {
      ...cleanEnv(process.env as Record<string, string | undefined>),
      ...cleanEnv(this.baseEnv),
      ...cleanEnv(input.env || {}),
      TERM: "xterm-256color",
    };

    const typeId = input.typeId || "terminal";

    mergedEnv.DBX_APP_TERMINAL_TYPE_ID = typeId;
    if (input.typeEntrypointPath) {
      mergedEnv.DBX_APP_TERMINAL_TYPE_ENTRYPOINT = input.typeEntrypointPath;
    } else {
      delete mergedEnv.DBX_APP_TERMINAL_TYPE_ENTRYPOINT;
    }

    const authMode: SessionAuthMode = input.authMode || "m2m";
    const auth: SessionAuthState = {
      mode: authMode,
      userAccessToken: input.userAccessToken,
      databricksHost: input.databricksHost,
      userAccessTokenHeader: input.userAccessTokenHeader,
      originalDatabricksHost: mergedEnv.DATABRICKS_HOST,
      originalDatabricksClientId: mergedEnv.DATABRICKS_CLIENT_ID,
      originalDatabricksClientSecret: mergedEnv.DATABRICKS_CLIENT_SECRET,
      stateFilePath: path.join(this.authStateDir, `${input.sessionId}.env`),
    };

    applyAuthModeToEnv(mergedEnv, auth);
    mergedEnv.DBX_APP_TERMINAL_AUTH_STATE_FILE = auth.stateFilePath;

    await this.persistAuthState(auth);

    let p: IPty;

    try {
      p = pty.spawn(this.shell, this.resolveShellArgs(), {
        name: "xterm-256color",
        cols,
        rows,
        cwd,
        env: mergedEnv,
      });
    } catch (error) {
      await this.deleteAuthStateFile(auth.stateFilePath);
      throw new AppError(500, "SESSION_SPAWN_FAILED", "Failed to spawn PTY session", true, {
        sessionId: input.sessionId,
        shell: this.shell,
        cause: error instanceof Error ? error.message : String(error),
      });
    }

    const info: SessionInfo = {
      sessionId: input.sessionId,
      createdAt: Date.now(),
      cwd,
      cols,
      rows,
      authMode,
      typeId,
      attachedClients: 0,
    };

    const record: SessionRecord = {
      info,
      pty: p,
      clients: new Map(),
      history: "",
      auth,
    };

    p.onData((data) => {
      record.history = (record.history + data).slice(-this.maxHistoryChars);
      for (const client of record.clients.values()) {
        client.onData(data);
      }
    });

    p.onExit(({ exitCode, signal }) => {
      for (const client of record.clients.values()) {
        client.onExit({ exitCode, signal });
      }

      this.sessions.delete(input.sessionId);
      void this.deleteAuthStateFile(record.auth.stateFilePath);

      this.logger.info("session.exit", {
        sessionId: input.sessionId,
        exitCode,
        signal,
      });
    });

    this.sessions.set(input.sessionId, record);

    this.logger.info("session.create", {
      sessionId: input.sessionId,
      cwd,
      cols,
      rows,
      authMode,
      typeId,
      hasCachedUserToken: Boolean(record.auth.userAccessToken),
    });

    return { ...info };
  }

  async sessionExists(sessionId: string): Promise<boolean> {
    assertValidSessionId(sessionId);
    return this.sessions.has(sessionId);
  }

  async ensureSessionExists(sessionId: string): Promise<void> {
    if (!(await this.sessionExists(sessionId))) {
      throw new AppError(404, "SESSION_NOT_FOUND", "Session not found", false, {
        sessionId,
      });
    }
  }

  async getSessionInfo(sessionId: string): Promise<SessionInfo> {
    const session = this.getSession(sessionId);
    return this.toSessionInfo(session);
  }

  async attachSession(sessionId: string, handlers: AttachHandlers): Promise<() => void> {
    const session = this.getSession(sessionId);

    const client: SessionClient = {
      id: randomUUID(),
      onData: handlers.onData,
      onExit: handlers.onExit,
      onAuthMode: handlers.onAuthMode,
    };

    session.clients.set(client.id, client);

    if (session.history) {
      handlers.onData(session.history);
    }

    handlers.onAuthMode?.(session.info.authMode);

    return () => {
      session.clients.delete(client.id);
    };
  }

  async setSessionAuthMode(sessionId: string, input: SetSessionAuthModeInput): Promise<SessionInfo> {
    const session = this.getSession(sessionId);

    if (input.mode === "user") {
      const userAccessToken = input.userAccessToken || session.auth.userAccessToken;
      if (!userAccessToken) {
        throw new AppError(
          400,
          "USER_ACCESS_TOKEN_MISSING",
          "User access token is required for user auth mode",
          false,
        );
      }

      const databricksHost = input.databricksHost || session.auth.databricksHost || session.auth.originalDatabricksHost;
      if (!databricksHost) {
        throw new AppError(
          500,
          "DATABRICKS_HOST_UNAVAILABLE",
          "Databricks host is not configured for user auth mode",
          true,
        );
      }

      session.auth.mode = "user";
      session.auth.userAccessToken = userAccessToken;
      session.auth.databricksHost = databricksHost;
      session.auth.userAccessTokenHeader = input.userAccessTokenHeader || session.auth.userAccessTokenHeader;
      session.info.authMode = "user";
    } else {
      if (input.userAccessToken) {
        session.auth.userAccessToken = input.userAccessToken;
      }
      if (input.databricksHost) {
        session.auth.databricksHost = input.databricksHost;
      }
      if (input.userAccessTokenHeader) {
        session.auth.userAccessTokenHeader = input.userAccessTokenHeader;
      }

      session.auth.mode = "m2m";
      session.info.authMode = "m2m";
    }

    await this.persistAuthState(session.auth);

    for (const client of session.clients.values()) {
      client.onAuthMode?.(session.info.authMode);
    }

    this.logger.info("session.auth_mode", {
      sessionId,
      mode: session.info.authMode,
      hasCachedUserToken: Boolean(session.auth.userAccessToken),
    });

    return this.toSessionInfo(session);
  }

  async writeInput(sessionId: string, data: string): Promise<void> {
    const session = this.getSession(sessionId);

    const bytes = Buffer.byteLength(data, "utf8");
    if (bytes > this.maxInputBytes) {
      throw new AppError(413, "INPUT_TOO_LARGE", "Input exceeds maximum allowed bytes", false, {
        maxInputBytes: this.maxInputBytes,
        bytes,
      });
    }

    session.pty.write(data);
  }

  async resizeSession(sessionId: string, cols: number, rows: number): Promise<void> {
    const session = this.getSession(sessionId);

    const boundedCols = clamp(cols, 20, this.maxCols);
    const boundedRows = clamp(rows, 10, this.maxRows);

    session.pty.resize(boundedCols, boundedRows);
    session.info.cols = boundedCols;
    session.info.rows = boundedRows;
  }

  async killSession(sessionId: string): Promise<void> {
    const session = this.getSession(sessionId);
    try {
      session.pty.kill();
    } finally {
      this.sessions.delete(sessionId);
      await this.deleteAuthStateFile(session.auth.stateFilePath);

      this.logger.info("session.kill", {
        sessionId,
      });
    }
  }

  getStats(): { activeSessions: number; attachedClients: number } {
    let attachedClients = 0;
    for (const session of this.sessions.values()) {
      attachedClients += session.clients.size;
    }

    return {
      activeSessions: this.sessions.size,
      attachedClients,
    };
  }

  async shutdown(): Promise<void> {
    const ids = [...this.sessions.keys()];
    for (const sessionId of ids) {
      try {
        await this.killSession(sessionId);
      } catch {
        // ignore shutdown race
      }
    }
  }

  private resolveShellArgs(): string[] {
    if (this.bashRcFilePath && isBashShell(this.shell)) {
      return ["--rcfile", this.bashRcFilePath, "-i"];
    }

    return ["-i"];
  }

  private async persistAuthState(state: SessionAuthState): Promise<void> {
    await writeAuthStateFile(state.stateFilePath, renderAuthState(state));
  }

  private async deleteAuthStateFile(filePath: string): Promise<void> {
    try {
      await fs.rm(filePath, { force: true });
    } catch {
      // ignore cleanup errors
    }
  }

  private toSessionInfo(session: SessionRecord): SessionInfo {
    return {
      ...session.info,
      attachedClients: session.clients.size,
    };
  }

  private getSession(sessionId: string): SessionRecord {
    assertValidSessionId(sessionId);

    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new AppError(404, "SESSION_NOT_FOUND", "Session not found", false, {
        sessionId,
      });
    }

    return session;
  }
}
