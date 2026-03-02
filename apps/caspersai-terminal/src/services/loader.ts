import path from "node:path";
import { pathToFileURL } from "node:url";
import type { Logger } from "../logging/logger.js";
import type { RuntimeService } from "./contracts.js";

function isRuntimeService(value: unknown): value is RuntimeService {
  const candidate = value as RuntimeService;
  return Boolean(
    candidate
      && typeof candidate === "object"
      && typeof candidate.name === "string"
      && typeof candidate.start === "function"
      && typeof candidate.stop === "function",
  );
}

function normalizeModuleSpecifier(specifier: string): string {
  if (specifier.startsWith(".") || specifier.startsWith("/")) {
    return pathToFileURL(path.resolve(process.cwd(), specifier)).href;
  }
  return specifier;
}

function extractServices(moduleValue: unknown): RuntimeService[] {
  const mod = moduleValue as Record<string, unknown>;

  const candidates: unknown[] = [];

  if (mod.default) {
    candidates.push(mod.default);
  }
  if (mod.service) {
    candidates.push(mod.service);
  }
  if (Array.isArray(mod.services)) {
    candidates.push(...mod.services);
  }

  const resolved = candidates.flatMap((candidate) => {
    if (Array.isArray(candidate)) {
      return candidate;
    }
    return [candidate];
  });

  return resolved.filter(isRuntimeService);
}

export async function loadRuntimeServices(
  moduleSpecifiers: string[],
  logger: Logger,
): Promise<RuntimeService[]> {
  const services: RuntimeService[] = [];

  for (const specifier of moduleSpecifiers) {
    const normalized = normalizeModuleSpecifier(specifier);

    try {
      const loaded = await import(normalized);
      const extracted = extractServices(loaded);

      if (extracted.length === 0) {
        logger.warn("service.load.empty", {
          module: specifier,
        });
        continue;
      }

      for (const service of extracted) {
        services.push(service);
      }

      logger.info("service.load", {
        module: specifier,
        count: extracted.length,
      });
    } catch (error) {
      logger.error("service.load_failed", {
        module: specifier,
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return services;
}
