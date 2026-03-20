import { createApp, lakebase, server, toPlugin } from "@databricks/appkit";
import { SupportPlugin } from "./support-plugin.js";

const support = toPlugin<typeof SupportPlugin, Record<string, never>, "support">(
  SupportPlugin,
  "support",
);

createApp({
  plugins: [lakebase(), support(), server()],
}).catch(console.error);
