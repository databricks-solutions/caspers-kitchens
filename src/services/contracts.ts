import type { AppConfig } from "../config.js";
import type { Logger } from "../logging/logger.js";

export type ServiceSignal = {
  ok: boolean;
  message: string;
  details?: Record<string, unknown>;
};

export type SessionEnvRequest = {
  sessionId: string;
  actor: string;
  cwd?: string;
  typeId?: string;
};

export type ServiceContext = {
  config: AppConfig;
  logger: Logger;
};

export interface RuntimeService {
  name: string;
  requiredForReadiness?: boolean;
  start(context: ServiceContext): Promise<void>;
  stop(context: ServiceContext): Promise<void>;
  health?(context: ServiceContext): Promise<ServiceSignal> | ServiceSignal;
  readiness?(context: ServiceContext): Promise<ServiceSignal> | ServiceSignal;
  enrichSessionEnv?(
    request: SessionEnvRequest,
    context: ServiceContext,
  ): Promise<Record<string, string> | void>;
}

export type ServiceCheck = {
  name: string;
  requiredForReadiness: boolean;
  ok: boolean;
  message: string;
  details?: Record<string, unknown>;
};
