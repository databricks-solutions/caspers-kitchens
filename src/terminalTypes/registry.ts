import fs from "node:fs/promises";
import path from "node:path";
import { z } from "zod";
import type { Logger } from "../logging/logger.js";
import type {
  ResolvedTerminalType,
  TerminalType,
  TerminalTypeAuthPolicy,
  TerminalTypeRegistry,
} from "./types.js";

const BASE_TERMINAL_TYPE: ResolvedTerminalType = {
  id: "terminal",
  name: "Terminal",
  description: "Plain shell session",
  badge: "terminal",
  icon: "⌂",
  authPolicy: "both",
  default: true,
  builtIn: true,
};

const typeIdPattern = /^[a-z0-9][a-z0-9-_]{0,63}$/;

const authPolicyValues = ["both", "user", "m2m"] as const satisfies readonly TerminalTypeAuthPolicy[];

const terminalTypeManifestSchema = z.object({
  id: z.string().regex(typeIdPattern).optional(),
  name: z.string().min(1).max(80),
  description: z.string().min(1).max(160).optional(),
  badge: z.string().min(1).max(24).optional(),
  icon: z.string().min(1).max(8).optional(),
  authPolicy: z.enum(authPolicyValues).optional(),
  default: z.boolean().optional(),
  order: z.number().int().min(-1_000_000).max(1_000_000).optional(),
  entrypoint: z.string().min(1).max(200).optional(),
});

type TerminalTypeManifest = z.infer<typeof terminalTypeManifestSchema>;

class InMemoryTerminalTypeRegistry implements TerminalTypeRegistry {
  constructor(
    private readonly types: ResolvedTerminalType[],
    private readonly typeById: Map<string, ResolvedTerminalType>,
    private readonly defaultType: ResolvedTerminalType,
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
    return { ...this.defaultType };
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

function hasExplicitOrder(type: ResolvedTerminalType): boolean {
  return typeof type.order === "number" && Number.isFinite(type.order);
}

function compareTerminalTypes(a: ResolvedTerminalType, b: ResolvedTerminalType): number {
  const aOrdered = hasExplicitOrder(a);
  const bOrdered = hasExplicitOrder(b);

  if (aOrdered && bOrdered) {
    const orderDiff = (a.order as number) - (b.order as number);
    if (orderDiff !== 0) {
      return orderDiff;
    }
  }

  if (aOrdered !== bOrdered) {
    return aOrdered ? -1 : 1;
  }

  if (a.default !== b.default) {
    return a.default ? -1 : 1;
  }

  const byName = a.name.localeCompare(b.name);
  if (byName !== 0) {
    return byName;
  }

  return a.id.localeCompare(b.id);
}

function resolveDefaults(
  types: ResolvedTerminalType[],
  terminalTypesRoot: string,
  logger: Logger,
): ResolvedTerminalType[] {
  const customDefaults = types.filter((type) => !type.builtIn && type.default);
  if (customDefaults.length === 0) {
    return types.map((type) => ({ ...type }));
  }

  const selected = [...customDefaults].sort(compareTerminalTypes)[0];

  if (customDefaults.length > 1) {
    logger.warn("terminal_types.multiple_defaults", {
      terminalTypesRoot,
      selectedId: selected.id,
      defaultIds: customDefaults.map((type) => type.id),
    });
  }

  return types.map((type) => ({
    ...type,
    default: type.id === selected.id,
  }));
}

export async function loadTerminalTypeRegistry(
  terminalTypesRoot: string,
  logger: Logger,
): Promise<TerminalTypeRegistry> {
  const baseType = { ...BASE_TERMINAL_TYPE };
  const map = new Map<string, ResolvedTerminalType>([[baseType.id, baseType]]);

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

    return new InMemoryTerminalTypeRegistry([baseType], map, baseType);
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
      authPolicy: manifest.authPolicy || "both",
      default: Boolean(manifest.default),
      order: manifest.order,
      builtIn: false,
      entrypointPath,
    });
  }

  const resolvedTypes = resolveDefaults([...map.values()], terminalTypesRoot, logger);
  const types = [...resolvedTypes].sort(compareTerminalTypes);
  const typeById = new Map(types.map((type) => [type.id, type]));
  const defaultType = types.find((type) => type.default) || baseType;

  logger.info("terminal_types.loaded", {
    terminalTypesRoot,
    count: types.length,
    customCount: Math.max(0, types.length - 1),
    defaultTypeId: defaultType.id,
  });

  return new InMemoryTerminalTypeRegistry(types, typeById, defaultType);
}
