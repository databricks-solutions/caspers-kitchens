import type pg from "pg";
import { createLakebasePool as createAppKitLakebasePool } from "@databricks/appkit";
import { WorkspaceClient } from "@databricks/sdk-experimental";

interface LakebaseEndpointResponse {
  status?: {
    hosts?: {
      host?: string;
    };
  };
}

async function resolveLakebaseHost(): Promise<string> {
  const configuredHost = process.env.PGHOST;
  if (configuredHost && configuredHost.trim().length > 0) {
    return configuredHost;
  }

  const endpoint = process.env.LAKEBASE_ENDPOINT;
  if (!endpoint || endpoint.trim().length === 0) {
    throw new Error("Set PGHOST or LAKEBASE_ENDPOINT for Lakebase connections");
  }

  const workspaceClient = new WorkspaceClient({
    host: process.env.DATABRICKS_HOST,
    ...(process.env.DATABRICKS_CONFIG_PROFILE && {
      profile: process.env.DATABRICKS_CONFIG_PROFILE,
    }),
  });

  const response = (await workspaceClient.apiClient.request({
    method: "GET",
    path: `/api/2.0/postgres/${endpoint}`,
    headers: new Headers({
      Accept: "application/json",
    }),
    raw: false,
  })) as LakebaseEndpointResponse;

  const resolvedHost = response?.status?.hosts?.host;
  if (!resolvedHost) {
    throw new Error(`Could not resolve Lakebase host from endpoint ${endpoint}`);
  }
  return resolvedHost;
}

export async function createLakebasePool(): Promise<pg.Pool> {
  const sslModeEnv = process.env.PGSSLMODE;
  const sslMode =
    sslModeEnv === "disable" || sslModeEnv === "prefer" || sslModeEnv === "require"
      ? sslModeEnv
      : "require";
  const strictSslVerify = process.env.PGSSL_STRICT_VERIFY === "true";
  const host = await resolveLakebaseHost();

  return createAppKitLakebasePool({
    host,
    sslMode,
    // Some Lakebase endpoint hosts include region segments not present in cert SANs.
    // Keep TLS on by default but allow hostname verification opt-in via PGSSL_STRICT_VERIFY=true.
    ssl: sslMode === "require" ? { rejectUnauthorized: strictSslVerify } : undefined,
  });
}
