export type ErrorDetails = Record<string, unknown>;

export type ApiErrorPayload = {
  code: string;
  message: string;
  retryable: boolean;
  requestId: string;
  details?: ErrorDetails;
};

export type ApiSuccess<T> = {
  ok: true;
  data: T;
};

export type ApiFailure = {
  ok: false;
  error: ApiErrorPayload;
};

export type ApiResponse<T> = ApiSuccess<T> | ApiFailure;

export class AppError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly retryable = false,
    public readonly details?: ErrorDetails,
  ) {
    super(message);
    this.name = "AppError";
  }
}

export function ok<T>(data: T): ApiSuccess<T> {
  return { ok: true, data };
}

export function fail(
  code: string,
  message: string,
  requestId: string,
  retryable = false,
  details?: ErrorDetails,
): ApiFailure {
  return {
    ok: false,
    error: {
      code,
      message,
      retryable,
      requestId,
      details,
    },
  };
}

export function toApiFailure(error: unknown, requestId: string): { status: number; body: ApiFailure } {
  if (error instanceof AppError) {
    return {
      status: error.status,
      body: fail(error.code, error.message, requestId, error.retryable, error.details),
    };
  }

  if (error instanceof Error) {
    return {
      status: 500,
      body: fail("INTERNAL_ERROR", error.message, requestId, false),
    };
  }

  return {
    status: 500,
    body: fail("INTERNAL_ERROR", "Unknown error", requestId, false),
  };
}
