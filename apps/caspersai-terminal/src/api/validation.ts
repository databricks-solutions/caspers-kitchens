import type { Request } from "express";
import { z, type ZodTypeAny } from "zod";
import { AppError } from "./types.js";

function normalizeIssues(error: z.ZodError): Array<{ path: string; message: string }> {
  return error.issues.map((issue) => ({
    path: issue.path.join(".") || "(root)",
    message: issue.message,
  }));
}

function parseWithSchema<T extends ZodTypeAny>(
  value: unknown,
  schema: T,
  section: "body" | "params" | "query",
): z.infer<T> {
  const result = schema.safeParse(value);
  if (!result.success) {
    throw new AppError(400, "INVALID_REQUEST", `Invalid ${section}`, false, {
      section,
      issues: normalizeIssues(result.error),
    });
  }

  return result.data;
}

export function parseBody<T extends ZodTypeAny>(req: Request, schema: T): z.infer<T> {
  return parseWithSchema(req.body, schema, "body");
}

export function parseParams<T extends ZodTypeAny>(req: Request, schema: T): z.infer<T> {
  return parseWithSchema(req.params, schema, "params");
}

export function parseQuery<T extends ZodTypeAny>(req: Request, schema: T): z.infer<T> {
  return parseWithSchema(req.query, schema, "query");
}
