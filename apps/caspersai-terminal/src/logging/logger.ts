import fs from "node:fs";
import path from "node:path";

export type LogLevel = "debug" | "info" | "warn" | "error";

const LEVEL_PRIORITY: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

export type LogFields = Record<string, unknown>;

export type LoggerOptions = {
  appName: string;
  level: LogLevel;
  path?: string;
};

export class Logger {
  private readonly appName: string;
  private readonly minLevel: LogLevel;
  private readonly stream: fs.WriteStream | null;

  constructor(options: LoggerOptions) {
    this.appName = options.appName;
    this.minLevel = options.level;

    if (options.path) {
      fs.mkdirSync(path.dirname(options.path), { recursive: true });
      this.stream = fs.createWriteStream(options.path, { flags: "a" });
    } else {
      this.stream = null;
    }
  }

  debug(event: string, fields?: LogFields): void {
    this.emit("debug", event, fields);
  }

  info(event: string, fields?: LogFields): void {
    this.emit("info", event, fields);
  }

  warn(event: string, fields?: LogFields): void {
    this.emit("warn", event, fields);
  }

  error(event: string, fields?: LogFields): void {
    this.emit("error", event, fields);
  }

  close(): void {
    if (this.stream) {
      this.stream.end();
    }
  }

  private emit(level: LogLevel, event: string, fields?: LogFields): void {
    if (LEVEL_PRIORITY[level] < LEVEL_PRIORITY[this.minLevel]) {
      return;
    }

    const payload = {
      ts: new Date().toISOString(),
      level,
      app: this.appName,
      event,
      ...fields,
    };

    const line = JSON.stringify(payload);
    if (level === "error") {
      // eslint-disable-next-line no-console
      console.error(line);
    } else {
      // eslint-disable-next-line no-console
      console.log(line);
    }

    if (this.stream) {
      this.stream.write(`${line}\n`);
    }
  }
}
