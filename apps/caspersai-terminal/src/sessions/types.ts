export type SessionAuthMode = "m2m" | "user";

export type SessionInfo = {
  sessionId: string;
  createdAt: number;
  cwd: string;
  cols: number;
  rows: number;
  authMode: SessionAuthMode;
  typeId: string;
  attachedClients: number;
};

export type SessionExit = {
  exitCode: number;
  signal?: number;
};

export type CreateSessionInput = {
  sessionId: string;
  cwd?: string;
  cols?: number;
  rows?: number;
  authMode?: SessionAuthMode;
  typeId?: string;
  typeEntrypointPath?: string;
  userAccessToken?: string;
  databricksHost?: string;
  userAccessTokenHeader?: string;
  env?: Record<string, string | undefined>;
};

export type SetSessionAuthModeInput = {
  mode: SessionAuthMode;
  userAccessToken?: string;
  databricksHost?: string;
  userAccessTokenHeader?: string;
};

export type AttachHandlers = {
  onData: (data: string) => void;
  onExit: (exit: SessionExit) => void;
  onAuthMode?: (mode: SessionAuthMode) => void;
};

export interface SessionManager {
  listSessions(): Promise<SessionInfo[]>;
  createSession(input: CreateSessionInput): Promise<SessionInfo>;
  sessionExists(sessionId: string): Promise<boolean>;
  ensureSessionExists(sessionId: string): Promise<void>;
  getSessionInfo(sessionId: string): Promise<SessionInfo>;
  attachSession(sessionId: string, handlers: AttachHandlers): Promise<() => void>;
  setSessionAuthMode(sessionId: string, input: SetSessionAuthModeInput): Promise<SessionInfo>;
  writeInput(sessionId: string, data: string): Promise<void>;
  resizeSession(sessionId: string, cols: number, rows: number): Promise<void>;
  killSession(sessionId: string): Promise<void>;
  getStats(): { activeSessions: number; attachedClients: number };
  shutdown(): Promise<void>;
}
