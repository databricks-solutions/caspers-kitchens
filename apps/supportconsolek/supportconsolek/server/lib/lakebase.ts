import type pg from "pg";
import { createLakebasePool as createAppKitLakebasePool } from "@databricks/appkit";

export function createLakebasePool(): pg.Pool {
  const sslModeEnv = process.env.PGSSLMODE;
  const sslMode =
    sslModeEnv === "disable" || sslModeEnv === "prefer" || sslModeEnv === "require"
      ? sslModeEnv
      : "require";
  const strictSslVerify = process.env.PGSSL_STRICT_VERIFY === "true";

  return createAppKitLakebasePool({
    sslMode,
    // Some Lakebase endpoint hosts include region segments not present in cert SANs.
    // Keep TLS on by default but allow hostname verification opt-in via PGSSL_STRICT_VERIFY=true.
    ssl: sslMode === "require" ? { rejectUnauthorized: strictSslVerify } : undefined,
  });
}
