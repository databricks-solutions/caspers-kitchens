import { createApp, lakebase, server, toPlugin } from "@databricks/appkit";
import { InvoicePlugin } from "./invoice-plugin.js";

const invoice = toPlugin<typeof InvoicePlugin, Record<string, never>, "invoice">(
  InvoicePlugin,
  "invoice",
);

createApp({
  plugins: [lakebase(), invoice(), server()],
}).catch(console.error);
