import fs from "node:fs/promises";
import path from "node:path";
import { z } from "zod";
import type { Logger } from "../logging/logger.js";
import type { ResolvedTerminalType, TerminalType, TerminalTypeRegistry } from "./types.js";

const BASE_TERMINAL_TYPE: ResolvedTerminalType = {
  id: "terminal",
  name: "Terminal",
  description: "Plain shell session",
  badge: "terminal",
  icon: "⌂",
  default: true,
  builtIn: true,
};

const typeIdPattern = /^[a-z0-9][a-z0-9-_]{0,63}$/;

const terminalTypeManifestSchema = z.object({
  id: z.string().regex(typeIdPattern).optional(),
  name: z.string().min(1).max(80),
  description: z.string().min(1).max(160).optional(),
  badge: z.string().min(1).max(24).optional(),
  icon: z.string().min(1).max(8).optional(),
  entrypoint: z.string().min(1).max(200).optional(),
});

type TerminalTypeManifest = z.infer<typeof terminalTypeManifestSchema>;

class InMemoryTerminalTypeRegistry implements TerminalTypeRegistry {
  constructor(
    private readonly types: ResolvedTerminalType[],
    private readonly typeById: Map<string, ResolvedTerminalType>,
  ) {}

  listTypes(): TerminalType[] {
    return this.types.map(({ entrypointPath: _entrypointPath, ...type }) => ({ ...type }));
  }

  resolveType(id: string): ResolvedTerminalType | undefined {
    const resolved = this.typeById.get(id);
    if (!resolved) {
      return undefined;
    }

    return { ...resolved };
  }

  getDefaultType(): ResolvedTerminalType {
    return { ...BASE_TERMINAL_TYPE };
  }
}

async function readManifest(manifestPath: string): Promise<TerminalTypeManifest | undefined> {
  try {
    const raw = await fs.readFile(manifestPath, "utf8");
    const parsed = JSON.parse(raw) as unknown;
    return terminalTypeManifestSchema.parse(parsed);
  } catch {
    return undefined;
  }
}

async function isEntrypointFile(candidatePath: string): Promise<boolean> {
  try {
    return (await fs.stat(candidatePath)).isFile();
  } catch {
    return false;
  }
}

export async function loadTerminalTypeRegistry(
  terminalTypesRoot: string,
  logger: Logger,
): Promise<TerminalTypeRegistry> {
  const map = new Map<string, ResolvedTerminalType>([[BASE_TERMINAL_TYPE.id, BASE_TERMINAL_TYPE]]);

  let entries: import("node:fs").Dirent[] = [];

  try {
    entries = await fs.readdir(terminalTypesRoot, {
      withFileTypes: true,
      encoding: "utf8",
    });
  } catch {
    logger.info("terminal_types.root_missing", {
      terminalTypesRoot,
    });

    return new InMemoryTerminalTypeRegistry([BASE_TERMINAL_TYPE], map);
  }

  for (const entry of entries) {
    if (!entry.isDirectory()) {
      continue;
    }

    const typeDir = path.join(terminalTypesRoot, entry.name);
    const manifestPath = path.join(typeDir, "type.json");

    const manifest = await readManifest(manifestPath);
    if (!manifest) {
      logger.warn("terminal_types.invalid_manifest", {
        terminalTypesRoot,
        manifestPath,
      });
      continue;
    }

    const id = manifest.id || entry.name;

    if (!typeIdPattern.test(id)) {
      logger.warn("terminal_types.invalid_id", {
        terminalTypesRoot,
        manifestPath,
        id,
      });
      continue;
    }

    if (id === BASE_TERMINAL_TYPE.id || map.has(id)) {
      logger.warn("terminal_types.duplicate_id", {
        terminalTypesRoot,
        manifestPath,
        id,
      });
      continue;
    }

    const entrypointPath = path.resolve(typeDir, manifest.entrypoint || "launch.sh");

    if (!(await isEntrypointFile(entrypointPath))) {
      logger.warn("terminal_types.entrypoint_missing", {
        terminalTypesRoot,
        manifestPath,
        entrypointPath,
      });
      continue;
    }

    map.set(id, {
      id,
      name: manifest.name,
      description: manifest.description,
      badge: manifest.badge || id,
      icon: manifest.icon,
      builtIn: false,
      default: false,
      entrypointPath,
    });
  }

  const types = [...map.values()].sort((a, b) => {
    if (a.default) {
      return -1;
    }
    if (b.default) {
      return 1;
    }
    return a.name.localeCompare(b.name);
  });

  logger.info("terminal_types.loaded", {
    terminalTypesRoot,
    count: types.length,
    customCount: Math.max(0, types.length - 1),
  });

  return new InMemoryTerminalTypeRegistry(types, map);
}
