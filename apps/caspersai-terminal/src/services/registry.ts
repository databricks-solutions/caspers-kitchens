import { AppError } from "../api/types.js";
import type { Logger } from "../logging/logger.js";
import type {
  RuntimeService,
  ServiceCheck,
  ServiceContext,
  ServiceSignal,
  SessionEnvRequest,
} from "./contracts.js";

type ServiceState = {
  started: boolean;
  startError?: string;
};

function normalizeSignal(value: ServiceSignal | undefined, fallback: string): ServiceSignal {
  if (!value) {
    return {
      ok: true,
      message: fallback,
    };
  }
  return value;
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  let timeout: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_resolve, reject) => {
        timeout = setTimeout(() => {
          reject(new AppError(504, "SERVICE_TIMEOUT", "Service hook timed out", true));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeout) {
      clearTimeout(timeout);
    }
  }
}

export class ServiceRegistry {
  private readonly services: RuntimeService[];
  private readonly states = new Map<string, ServiceState>();

  constructor(
    services: RuntimeService[],
    private readonly context: ServiceContext,
    private readonly logger: Logger,
    private readonly sessionEnvHookTimeoutMs: number,
  ) {
    this.services = services;

    for (const service of services) {
      this.states.set(service.name, {
        started: false,
      });
    }
  }

  getServiceCount(): number {
    return this.services.length;
  }

  async startAll(): Promise<void> {
    for (const service of this.services) {
      try {
        await service.start(this.context);
        this.states.set(service.name, { started: true });
        this.logger.info("service.start", {
          service: service.name,
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        this.states.set(service.name, { started: false, startError: message });
        this.logger.error("service.start_failed", {
          service: service.name,
          message,
        });
      }
    }
  }

  async stopAll(): Promise<void> {
    for (const service of [...this.services].reverse()) {
      try {
        await service.stop(this.context);
        this.logger.info("service.stop", {
          service: service.name,
        });
      } catch (error) {
        this.logger.error("service.stop_failed", {
          service: service.name,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }

  async collectHealth(): Promise<ServiceCheck[]> {
    const checks = await Promise.all(
      this.services.map(async (service): Promise<ServiceCheck> => {
        const state = this.states.get(service.name);
        if (!state?.started) {
          return {
            name: service.name,
            requiredForReadiness: Boolean(service.requiredForReadiness),
            ok: false,
            message: state?.startError || "service not started",
          };
        }

        if (!service.health) {
          return {
            name: service.name,
            requiredForReadiness: Boolean(service.requiredForReadiness),
            ok: true,
            message: "started",
          };
        }

        try {
          const signal = normalizeSignal(await service.health(this.context), "healthy");
          return {
            name: service.name,
            requiredForReadiness: Boolean(service.requiredForReadiness),
            ok: signal.ok,
            message: signal.message,
            details: signal.details,
          };
        } catch (error) {
          return {
            name: service.name,
            requiredForReadiness: Boolean(service.requiredForReadiness),
            ok: false,
            message: error instanceof Error ? error.message : String(error),
          };
        }
      }),
    );

    return checks;
  }

  async collectReadiness(): Promise<ServiceCheck[]> {
    const checks = await Promise.all(
      this.services.map(async (service): Promise<ServiceCheck> => {
        const state = this.states.get(service.name);
        if (!state?.started) {
          return {
            name: service.name,
            requiredForReadiness: Boolean(service.requiredForReadiness),
            ok: false,
            message: state?.startError || "service not started",
          };
        }

        if (!service.readiness) {
          return {
            name: service.name,
            requiredForReadiness: Boolean(service.requiredForReadiness),
            ok: true,
            message: "ready",
          };
        }

        try {
          const signal = normalizeSignal(await service.readiness(this.context), "ready");
          return {
            name: service.name,
            requiredForReadiness: Boolean(service.requiredForReadiness),
            ok: signal.ok,
            message: signal.message,
            details: signal.details,
          };
        } catch (error) {
          return {
            name: service.name,
            requiredForReadiness: Boolean(service.requiredForReadiness),
            ok: false,
            message: error instanceof Error ? error.message : String(error),
          };
        }
      }),
    );

    return checks;
  }

  async buildSessionEnv(request: SessionEnvRequest): Promise<{ env: Record<string, string>; warnings: string[] }> {
    const enrichers = this.services.filter((service) => {
      if (!service.enrichSessionEnv) {
        return false;
      }

      const state = this.states.get(service.name);
      return Boolean(state?.started);
    });

    if (enrichers.length === 0) {
      return { env: {}, warnings: [] };
    }

    const warnings: string[] = [];

    const fragments = await Promise.all(
      enrichers.map(async (service) => {
        try {
          const result = await withTimeout(
            Promise.resolve(service.enrichSessionEnv?.(request, this.context)),
            this.sessionEnvHookTimeoutMs,
          );
          if (!result) {
            return {};
          }
          return result;
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          const warning = `${service.name}: ${message}`;
          warnings.push(warning);
          this.logger.warn("service.session_env_enrich_failed", {
            service: service.name,
            sessionId: request.sessionId,
            message,
          });
          return {};
        }
      }),
    );

    return {
      env: Object.assign({}, ...fragments),
      warnings,
    };
  }
}
