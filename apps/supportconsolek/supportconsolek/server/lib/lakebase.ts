import type pg from "pg";
import { createLakebasePool as createAppKitLakebasePool } from "@databricks/appkit";

export function createLakebasePool(): pg.Pool {
  return createAppKitLakebasePool();
}
