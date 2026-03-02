import type { IncomingMessage, Server as HttpServer } from "node:http";
import type { Duplex } from "node:stream";
import { URL } from "node:url";
import { WebSocketServer, type WebSocket } from "ws";
import { AppError } from "../api/types.js";
import type { AppConfig } from "../config.js";
import type { Logger } from "../logging/logger.js";
import { assertValidSessionId } from "../sessions/ptySessionManager.js";
import type { SessionManager, SessionAuthMode, SessionExit } from "../sessions/types.js";

type ClientMessage =
  | { type: "input"; data: string }
  | { type: "resize"; cols: number; rows: number }
  | { type: "ping" };

type ServerMessage =
  | { type: "ready"; sessionId: string }
  | { type: "output"; data: string }
  | { type: "error"; message: string }
  | { type: "exit"; exitCode: number; signal?: number }
  | { type: "auth_mode"; mode: SessionAuthMode }
  | { type: "pong" };

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function send(
  ws: WebSocket,
  payload: ServerMessage,
  logger: Logger,
  wsBackpressureBytes: number,
  sessionId: string,
): void {
  if (ws.readyState !== ws.OPEN) {
    return;
  }

  if (ws.bufferedAmount > wsBackpressureBytes) {
    logger.warn("ws.backpressure_close", {
      sessionId,
      bufferedAmount: ws.bufferedAmount,
      threshold: wsBackpressureBytes,
    });
    ws.close(1009, "client-too-slow");
    return;
  }

  ws.send(JSON.stringify(payload));
}

function parseConnection(
  req: IncomingMessage,
  config: AppConfig,
): {
  sessionId: string;
  cols: number;
  rows: number;
} {
  const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
  const sessionId = url.searchParams.get("sessionId") || "";

  const colsRaw = Number(url.searchParams.get("cols") || config.defaultCols);
  const rowsRaw = Number(url.searchParams.get("rows") || config.defaultRows);

  assertValidSessionId(sessionId);

  return {
    sessionId,
    cols: clamp(Number.isFinite(colsRaw) ? colsRaw : config.defaultCols, 20, config.maxCols),
    rows: clamp(Number.isFinite(rowsRaw) ? rowsRaw : config.defaultRows, 10, config.maxRows),
  };
}

export class TerminalGateway {
  private readonly wss: WebSocketServer;

  constructor(
    server: HttpServer,
    private readonly config: AppConfig,
    private readonly logger: Logger,
    private readonly sessions: SessionManager,
  ) {
    this.wss = new WebSocketServer({
      noServer: true,
      maxPayload: config.maxWsMessageBytes,
    });

    this.wss.on("connection", (ws, req) => {
      void this.handleConnection(ws, req);
    });

    server.on("upgrade", (req, socket, head) => {
      this.handleUpgrade(req, socket, head);
    });
  }

  private handleUpgrade(req: IncomingMessage, socket: Duplex, head: Buffer): void {
    if (!req.url?.startsWith("/ws/terminal")) {
      socket.destroy();
      return;
    }

    this.wss.handleUpgrade(req, socket, head, (ws) => {
      this.wss.emit("connection", ws, req);
    });
  }

  private async handleConnection(ws: WebSocket, req: IncomingMessage): Promise<void> {
    let sessionId = "unknown";
    let cols = this.config.defaultCols;
    let rows = this.config.defaultRows;

    try {
      const parsed = parseConnection(req, this.config);
      sessionId = parsed.sessionId;
      cols = parsed.cols;
      rows = parsed.rows;
    } catch (error) {
      send(
        ws,
        {
          type: "error",
          message: error instanceof Error ? error.message : "Invalid connection request",
        },
        this.logger,
        this.config.wsBackpressureBytes,
        sessionId,
      );
      ws.close(1008, "invalid-connection");
      return;
    }

    try {
      await this.sessions.ensureSessionExists(sessionId);
      await this.sessions.resizeSession(sessionId, cols, rows);
    } catch (error) {
      send(
        ws,
        {
          type: "error",
          message: error instanceof Error ? error.message : "Session unavailable",
        },
        this.logger,
        this.config.wsBackpressureBytes,
        sessionId,
      );
      ws.close(1008, "session-unavailable");
      return;
    }

    const detach = await this.sessions.attachSession(sessionId, {
      onData: (data) => {
        send(
          ws,
          {
            type: "output",
            data,
          },
          this.logger,
          this.config.wsBackpressureBytes,
          sessionId,
        );
      },
      onExit: ({ exitCode, signal }: SessionExit) => {
        send(
          ws,
          {
            type: "exit",
            exitCode,
            signal,
          },
          this.logger,
          this.config.wsBackpressureBytes,
          sessionId,
        );
      },
      onAuthMode: (mode: SessionAuthMode) => {
        send(
          ws,
          {
            type: "auth_mode",
            mode,
          },
          this.logger,
          this.config.wsBackpressureBytes,
          sessionId,
        );
      },
    });

    this.logger.info("ws.connect", {
      sessionId,
      remoteAddress: req.socket.remoteAddress,
      cols,
      rows,
    });

    send(
      ws,
      {
        type: "ready",
        sessionId,
      },
      this.logger,
      this.config.wsBackpressureBytes,
      sessionId,
    );

    let alive = true;
    ws.on("pong", () => {
      alive = true;
    });

    const heartbeat = setInterval(() => {
      if (!alive) {
        ws.terminate();
        return;
      }

      alive = false;
      ws.ping();
    }, 15_000);

    ws.on("message", (raw) => {
      let message: ClientMessage;
      try {
        message = JSON.parse(raw.toString()) as ClientMessage;
      } catch {
        send(
          ws,
          {
            type: "error",
            message: "Invalid websocket payload",
          },
          this.logger,
          this.config.wsBackpressureBytes,
          sessionId,
        );
        return;
      }

      if (message.type === "input") {
        void this.sessions.writeInput(sessionId, message.data).catch((error) => {
          const msg = error instanceof Error ? error.message : String(error);
          this.logger.warn("ws.input_error", {
            sessionId,
            message: msg,
          });

          if (error instanceof AppError && error.code === "SESSION_NOT_FOUND") {
            send(
              ws,
              {
                type: "error",
                message: "Session no longer exists",
              },
              this.logger,
              this.config.wsBackpressureBytes,
              sessionId,
            );
            ws.close(1011, "session-lost");
            return;
          }

          send(
            ws,
            {
              type: "error",
              message: msg,
            },
            this.logger,
            this.config.wsBackpressureBytes,
            sessionId,
          );
        });
        return;
      }

      if (message.type === "resize") {
        cols = clamp(message.cols, 20, this.config.maxCols);
        rows = clamp(message.rows, 10, this.config.maxRows);

        void this.sessions.resizeSession(sessionId, cols, rows).catch((error) => {
          this.logger.warn("ws.resize_error", {
            sessionId,
            message: error instanceof Error ? error.message : String(error),
          });
        });
        return;
      }

      if (message.type === "ping") {
        send(
          ws,
          {
            type: "pong",
          },
          this.logger,
          this.config.wsBackpressureBytes,
          sessionId,
        );
      }
    });

    ws.on("close", () => {
      clearInterval(heartbeat);
      detach();
      this.logger.info("ws.disconnect", {
        sessionId,
      });
    });

    ws.on("error", (error) => {
      this.logger.warn("ws.error", {
        sessionId,
        message: error.message,
      });
    });
  }
}
