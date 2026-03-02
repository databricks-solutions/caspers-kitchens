import { AppError } from "../src/api/types.js";
import type {
  AttachHandlers,
  CreateSessionInput,
  SessionInfo,
  SessionManager,
  SetSessionAuthModeInput,
} from "../src/sessions/types.js";

type SessionRecord = {
  info: SessionInfo;
  clients: Set<AttachHandlers>;
  userAccessToken?: string;
  databricksHost?: string;
};

function now(): number {
  return Date.now();
}

export class FakeSessionManager implements SessionManager {
  private readonly sessions = new Map<string, SessionRecord>();

  readonly creates: CreateSessionInput[] = [];

  readonly writes: Array<{ sessionId: string; data: string }> = [];

  readonly resizes: Array<{ sessionId: string; cols: number; rows: number }> = [];

  async listSessions(): Promise<SessionInfo[]> {
    return [...this.sessions.values()].map((session) => ({
      ...session.info,
      attachedClients: session.clients.size,
    }));
  }

  async createSession(input: CreateSessionInput): Promise<SessionInfo> {
    if (this.sessions.has(input.sessionId)) {
      throw new AppError(409, "SESSION_EXISTS", "Session already exists", false, {
        sessionId: input.sessionId,
      });
    }

    this.creates.push(input);

    const info: SessionInfo = {
      sessionId: input.sessionId,
      createdAt: now(),
      cwd: input.cwd || process.cwd(),
      cols: input.cols || 120,
      rows: input.rows || 30,
      authMode: input.authMode || "m2m",
      typeId: input.typeId || "terminal",
      attachedClients: 0,
    };

    this.sessions.set(input.sessionId, {
      info,
      clients: new Set(),
      userAccessToken: input.userAccessToken,
      databricksHost: input.databricksHost,
    });

    return info;
  }

  async sessionExists(sessionId: string): Promise<boolean> {
    return this.sessions.has(sessionId);
  }

  async ensureSessionExists(sessionId: string): Promise<void> {
    if (!this.sessions.has(sessionId)) {
      throw new AppError(404, "SESSION_NOT_FOUND", "Session not found", false, {
        sessionId,
      });
    }
  }

  async getSessionInfo(sessionId: string): Promise<SessionInfo> {
    const record = this.sessions.get(sessionId);
    if (!record) {
      throw new AppError(404, "SESSION_NOT_FOUND", "Session not found", false, {
        sessionId,
      });
    }

    return {
      ...record.info,
      attachedClients: record.clients.size,
    };
  }

  async attachSession(sessionId: string, handlers: AttachHandlers): Promise<() => void> {
    const record = this.sessions.get(sessionId);
    if (!record) {
      throw new AppError(404, "SESSION_NOT_FOUND", "Session not found", false, {
        sessionId,
      });
    }

    record.clients.add(handlers);
    handlers.onAuthMode?.(record.info.authMode);

    return () => {
      record.clients.delete(handlers);
    };
  }

  async setSessionAuthMode(sessionId: string, input: SetSessionAuthModeInput): Promise<SessionInfo> {
    const record = this.sessions.get(sessionId);
    if (!record) {
      throw new AppError(404, "SESSION_NOT_FOUND", "Session not found", false, {
        sessionId,
      });
    }

    if (input.mode === "user") {
      const token = input.userAccessToken || record.userAccessToken;
      if (!token) {
        throw new AppError(400, "USER_ACCESS_TOKEN_MISSING", "User access token is required", false);
      }

      const host = input.databricksHost || record.databricksHost;
      if (!host) {
        throw new AppError(500, "DATABRICKS_HOST_UNAVAILABLE", "Databricks host is required", true);
      }

      record.userAccessToken = token;
      record.databricksHost = host;
      record.info.authMode = "user";
    } else {
      if (input.userAccessToken) {
        record.userAccessToken = input.userAccessToken;
      }
      if (input.databricksHost) {
        record.databricksHost = input.databricksHost;
      }
      record.info.authMode = "m2m";
    }

    for (const client of record.clients) {
      client.onAuthMode?.(record.info.authMode);
    }

    return {
      ...record.info,
      attachedClients: record.clients.size,
    };
  }

  async writeInput(sessionId: string, data: string): Promise<void> {
    const record = this.sessions.get(sessionId);
    if (!record) {
      throw new AppError(404, "SESSION_NOT_FOUND", "Session not found", false, {
        sessionId,
      });
    }

    this.writes.push({ sessionId, data });

    for (const client of record.clients) {
      client.onData(data);
    }
  }

  async resizeSession(sessionId: string, cols: number, rows: number): Promise<void> {
    const record = this.sessions.get(sessionId);
    if (!record) {
      throw new AppError(404, "SESSION_NOT_FOUND", "Session not found", false, {
        sessionId,
      });
    }

    record.info.cols = cols;
    record.info.rows = rows;
    this.resizes.push({ sessionId, cols, rows });
  }

  async killSession(sessionId: string): Promise<void> {
    const record = this.sessions.get(sessionId);
    if (!record) {
      throw new AppError(404, "SESSION_NOT_FOUND", "Session not found", false, {
        sessionId,
      });
    }

    for (const client of record.clients) {
      client.onExit({ exitCode: 0 });
    }

    this.sessions.delete(sessionId);
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
      await this.killSession(sessionId);
    }
  }
}
