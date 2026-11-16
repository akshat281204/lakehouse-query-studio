import type {
  ApiErrorBody,
  DatasetDetail,
  DatasetListResponse,
  HealthResponse,
  IngestionJob,
  IngestionListResponse,
  InspectResponse,
  PreviewResponse,
  ReadyResponse,
  SchemaResponse,
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

export interface ListDatasetsOptions {
  offset?: number
  limit?: number
  q?: string
}

export interface RegisterDatasetInput {
  name: string
  prefix: string
  format?: string
  partitionColumns?: string[]
}

export interface UploadOptions {
  name?: string
  partitionColumns?: string[]
  retainOriginal?: boolean
}

function uploadQuery(file: File, opts: UploadOptions): string {
  const params = new URLSearchParams()
  params.set('filename', file.name)
  if (opts.name) params.set('name', opts.name)
  if (opts.partitionColumns?.length) {
    params.set('partitionColumns', opts.partitionColumns.join(','))
  }
  if (opts.retainOriginal) params.set('retainOriginal', 'true')
  return `?${params.toString()}`
}

function listDatasetsQuery(opts: ListDatasetsOptions): string {
  const params = new URLSearchParams()
  if (opts.offset !== undefined) params.set('offset', String(opts.offset))
  if (opts.limit !== undefined) params.set('limit', String(opts.limit))
  if (opts.q) params.set('q', opts.q)
  const qs = params.toString()
  return qs ? `?${qs}` : ''
}

/**
 * Typed client for the Lakehouse Query Studio API.
 */
export const api = {
  serviceInfo: () => request<ServiceInfo>('/api/v1/info'),
  health: () => request<HealthResponse>('/healthz'),
  ready: () => request<ReadyResponse>('/readyz'),

  listDatasets: (opts: ListDatasetsOptions = {}) =>
    request<DatasetListResponse>(`/api/v1/datasets${listDatasetsQuery(opts)}`),

  registerDataset: (input: RegisterDatasetInput) =>
    request<DatasetDetail>('/api/v1/datasets', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(input),
    }),

  getDataset: (id: string) =>
    request<DatasetDetail>(`/api/v1/datasets/${encodeURIComponent(id)}`),

  getDatasetSchema: (id: string) =>
    request<SchemaResponse>(
      `/api/v1/datasets/${encodeURIComponent(id)}/schema`,
    ),

  getDatasetPreview: (id: string, limit?: number) => {
    const qs = limit ? `?limit=${limit}` : ''
    return request<PreviewResponse>(
      `/api/v1/datasets/${encodeURIComponent(id)}/preview${qs}`,
    )
  },

  inspectUpload: (file: File) =>
    request<InspectResponse>(
      `/api/v1/ingestions/inspect${uploadQuery(file, {})}`,
      { method: 'POST', body: file },
    ),

  uploadDataset: (file: File, opts: UploadOptions) =>
    request<IngestionJob>(`/api/v1/ingestions${uploadQuery(file, opts)}`, {
      method: 'POST',
      body: file,
    }),

  listIngestions: () => request<IngestionListResponse>('/api/v1/ingestions'),

  getIngestion: (id: string) =>
    request<IngestionJob>(`/api/v1/ingestions/${encodeURIComponent(id)}`),

  retryIngestion: (id: string) =>
    request<IngestionJob>(
      `/api/v1/ingestions/${encodeURIComponent(id)}/retry`,
      { method: 'POST' },
    ),
}
