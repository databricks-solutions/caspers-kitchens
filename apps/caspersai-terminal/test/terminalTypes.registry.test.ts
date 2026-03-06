import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs/promises";
import { Logger } from "../src/logging/logger.js";
import { loadTerminalTypeRegistry } from "../src/terminalTypes/registry.js";

type TypeManifest = {
  id?: string;
  name: string;
  description?: string;
  badge?: string;
  icon?: string;
  authPolicy?: "both" | "user" | "m2m";
  default?: boolean;
  order?: number;
  entrypoint?: string;
};

function makeLogger(): Logger {
  return new Logger({
    appName: "databricks-app-terminal-test",
    level: "error",
  });
}

async function writeTerminalType(root: string, folderName: string, manifest: TypeManifest): Promise<void> {
  const typeDir = path.join(root, folderName);
  await fs.mkdir(typeDir, { recursive: true });

  await fs.writeFile(path.join(typeDir, "type.json"), JSON.stringify(manifest, null, 2));

  const entrypoint = manifest.entrypoint || "launch.sh";
  await fs.writeFile(path.join(typeDir, entrypoint), "#!/usr/bin/env bash\n");
}

test("custom default terminal type overrides built-in terminal default", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "dbx-terminal-types-"));
  t.after(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  await writeTerminalType(root, "caspersai", {
    name: "CaspersAI",
    authPolicy: "user",
    default: true,
  });

  const registry = await loadTerminalTypeRegistry(root, makeLogger());

  const defaultType = registry.getDefaultType();
  assert.equal(defaultType.id, "caspersai");

  const listed = registry.listTypes();
  const defaults = listed.filter((type) => type.default);
  assert.equal(defaults.length, 1);
  assert.equal(defaults[0]?.id, "caspersai");

  const terminal = listed.find((type) => type.id === "terminal");
  assert.equal(terminal?.default, false);
});

test("order field controls session type ordering", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "dbx-terminal-types-"));
  t.after(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  await writeTerminalType(root, "third", {
    name: "Third",
    order: 30,
  });

  await writeTerminalType(root, "first", {
    name: "First",
    order: -10,
  });

  await writeTerminalType(root, "unordered", {
    name: "Unordered",
  });

  const registry = await loadTerminalTypeRegistry(root, makeLogger());
  const ids = registry.listTypes().map((type) => type.id);

  assert.deepEqual(ids.slice(0, 4), ["first", "third", "terminal", "unordered"]);
});

test("multiple custom defaults resolve to one selected by ordering", async (t) => {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), "dbx-terminal-types-"));
  t.after(async () => {
    await fs.rm(root, { recursive: true, force: true });
  });

  await writeTerminalType(root, "slow-default", {
    name: "Slow Default",
    default: true,
    order: 100,
  });

  await writeTerminalType(root, "fast-default", {
    name: "Fast Default",
    default: true,
    order: 1,
  });

  const registry = await loadTerminalTypeRegistry(root, makeLogger());

  const defaultType = registry.getDefaultType();
  assert.equal(defaultType.id, "fast-default");

  const listed = registry.listTypes();
  const defaults = listed.filter((type) => type.default);
  assert.equal(defaults.length, 1);
  assert.equal(defaults[0]?.id, "fast-default");
});
