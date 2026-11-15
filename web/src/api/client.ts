import type {
  ApiErrorBody,
  HealthResponse,
  ReadyResponse,
  ServiceInfo,
} from './types'

/** An error carrying the API's stable error code and HTTP status. */
export class ApiError extends Error {
  readonly status: number
  readonly code: string

  constructor(status: number, code: string, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }
}

const BASE_URL = (import.meta.env.VITE_API_BASE_URL as string | undefined) ?? ''

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response
  try {
    res = await fetch(`${BASE_URL}${path}`, init)
  } catch (err) {
    throw new ApiError(0, 'network', `request failed: ${String(err)}`)
  }

  if (!res.ok) {
    let code = 'internal'
    let message = res.statusText || 'request failed'
    try {
      const body = (await res.json()) as ApiErrorBody
      code = body?.error?.code ?? code
      message = body?.error?.message ?? message
    } catch {
      // A non-JSON error body keeps the status-based fallback above.
    }
    throw new ApiError(res.status, code, message)
  }

  return (await res.json()) as T
}

/**
 * Typed client for the Lakehouse Query Studio API. Phase 1 exposes the service
 * root and the liveness/readiness probes; dataset and query endpoints are added
 * in later phases.
 */
export const api = {
  serviceInfo: () => request<ServiceInfo>('/api/v1/info'),
  health: () => request<HealthResponse>('/healthz'),
  ready: () => request<ReadyResponse>('/readyz'),
}
