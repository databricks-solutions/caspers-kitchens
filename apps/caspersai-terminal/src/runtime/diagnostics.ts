import { spawn } from "node:child_process";
import pty from "node-pty";
import type { AppConfig } from "../config.js";
import type { Logger } from "../logging/logger.js";
import type { SessionManager } from "../sessions/types.js";
import type { ServiceCheck } from "../services/contracts.js";
import type { ServiceRegistry } from "../services/registry.js";

export type RuntimeCheck = {
  name: "shell" | "pty";
  ok: boolean;
  fatal: boolean;
  message: string;
  details?: Record<string, unknown>;
};

export type RuntimeDiagnostics = {
  generatedAt: string;
  uptimeSec: number;
  coreReady: boolean;
  requiredServicesReady: boolean;
  ready: boolean;
  coreChecks: RuntimeCheck[];
  services: {
    health: ServiceCheck[];
    readiness: ServiceCheck[];
  };
  sessions: {
    activeSessions: number;
    attachedClients: number;
  };
};

type CommandResult = {
  code: number;
  stdout: string;
  stderr: string;
};

function runCommand(shell: string, command: string, timeoutMs: number): Promise<CommandResult> {
  return new Promise((resolve) => {
    const child = spawn(shell, ["-lc", command], {
      env: process.env,
    });

    let stdout = "";
    let stderr = "";

    const timer = setTimeout(() => {
      child.kill();
      resolve({
        code: 1,
        stdout: stdout.trim(),
        stderr: `command timeout after ${timeoutMs}ms`,
      });
    }, timeoutMs);

    child.stdout.on("data", (chunk: Buffer) => {
      stdout += chunk.toString("utf8");
    });

    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf8");
    });

    child.on("close", (code) => {
      clearTimeout(timer);
      resolve({
        code: code ?? 1,
        stdout: stdout.trim(),
        stderr: stderr.trim(),
      });
    });

    child.on("error", (error) => {
      clearTimeout(timer);
      resolve({
        code: 1,
        stdout: stdout.trim(),
        stderr: error.message,
      });
    });
  });
}

function runPtyProbe(shell: string, timeoutMs: number): Promise<CommandResult> {
  return new Promise((resolve) => {
    try {
      const p = pty.spawn(shell, ["-lc", "echo pty_ok && exit"], {
        name: "xterm-256color",
        cols: 80,
        rows: 24,
        cwd: process.cwd(),
        env: {
          ...process.env,
          TERM: "xterm-256color",
        },
      });

      let stdout = "";
      let settled = false;

      const settle = (result: CommandResult): void => {
        if (settled) {
          return;
        }
        settled = true;
        resolve(result);
      };

      const timer = setTimeout(() => {
        try {
          p.kill();
        } catch {
          // ignore
        }
        settle({
          code: 1,
          stdout: stdout.trim(),
          stderr: `pty timeout after ${timeoutMs}ms`,
        });
      }, timeoutMs);

      p.onData((data) => {
        stdout += data;
      });

      p.onExit(({ exitCode }) => {
        clearTimeout(timer);
        settle({
          code: exitCode,
          stdout: stdout.trim(),
          stderr: "",
        });
      });
    } catch (error) {
      resolve({
        code: 1,
        stdout: "",
        stderr: error instanceof Error ? error.message : String(error),
      });
    }
  });
}

export class RuntimeDiagnosticsManager {
  private cache: { at: number; data: RuntimeDiagnostics } | null = null;

  constructor(
    private readonly config: AppConfig,
    private readonly sessions: SessionManager,
    private readonly services: ServiceRegistry,
    private readonly logger: Logger,
  ) {}

  async getDiagnostics(refresh = false): Promise<RuntimeDiagnostics> {
    const now = Date.now();

    if (!refresh && this.cache && now - this.cache.at < this.config.diagnosticsTtlMs) {
      return this.cache.data;
    }

    const data = await this.collect();
    this.cache = {
      at: now,
      data,
    };

    return data;
  }

  private async collect(): Promise<RuntimeDiagnostics> {
    const [shellProbe, ptyProbe, health, readiness] = await Promise.all([
      runCommand(this.config.shell, "echo shell_ok", 2_000),
      runPtyProbe(this.config.shell, 2_000),
      this.services.collectHealth(),
      this.services.collectReadiness(),
    ]);

    const shellOk = shellProbe.code === 0 && shellProbe.stdout.includes("shell_ok");
    const ptyOk = ptyProbe.code === 0 && ptyProbe.stdout.includes("pty_ok");

    const coreChecks: RuntimeCheck[] = [
      {
        name: "shell",
        fatal: true,
        ok: shellOk,
        message: shellOk ? `Shell available (${this.config.shell})` : `Shell unavailable (${this.config.shell})`,
        details: shellOk ? undefined : { stderr: shellProbe.stderr },
      },
      {
        name: "pty",
        fatal: true,
        ok: ptyOk,
        message: ptyOk ? "node-pty probe succeeded" : "node-pty probe failed",
        details: ptyOk
          ? undefined
          : {
              stdout: ptyProbe.stdout,
              stderr: ptyProbe.stderr,
            },
      },
    ];

    const coreReady = coreChecks.filter((check) => check.fatal).every((check) => check.ok);
    const requiredServicesReady = readiness
      .filter((check) => check.requiredForReadiness)
      .every((check) => check.ok);

    const ready = coreReady && requiredServicesReady;

    const diagnostics: RuntimeDiagnostics = {
      generatedAt: new Date().toISOString(),
      uptimeSec: Math.round(process.uptime()),
      coreReady,
      requiredServicesReady,
      ready,
      coreChecks,
      services: {
        health,
        readiness,
      },
      sessions: this.sessions.getStats(),
    };

    this.logger.debug("runtime.diagnostics", {
      ready,
      coreReady,
      requiredServicesReady,
      activeSessions: diagnostics.sessions.activeSessions,
      attachedClients: diagnostics.sessions.attachedClients,
    });

    return diagnostics;
  }
}
