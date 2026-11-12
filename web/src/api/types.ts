export interface HealthResponse {
  status: 'ok'
}

export interface DependencyStatus {
  ok: boolean
  message?: string
}

export interface ReadyResponse {
  status: 'ok' | 'not_ready'
  dependencies: Record<string, DependencyStatus>
}

export interface ServiceInfo {
  service: string
  version: string
}

/** The stable JSON error envelope returned by the API for all non-2xx responses. */
export interface ApiErrorBody {
  error: {
    code: string
    message: string
    details?: Record<string, unknown>
  }
}
