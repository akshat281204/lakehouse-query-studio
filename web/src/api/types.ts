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

export interface Dataset {
  id: string
  namespace: string
  name: string
  sourcePrefix: string
  format: string
  status: string
  retainOriginal: boolean
  createdAt: string
  updatedAt: string
}

export interface DatasetVersion {
  id: string
  datasetId: string
  version: number
  objectPrefix: string
  format: string
  partitionColumns: string[]
  status: string
  errorSummary?: string
  rowCount?: number
  createdAt: string
}

export interface SchemaColumn {
  position: number
  name: string
  type: string
  nullable: boolean
  isPartition: boolean
}

export interface DatasetDetail extends Dataset {
  version: DatasetVersion
  schema: SchemaColumn[]
}

export interface DatasetListItem {
  id: string
  namespace: string
  name: string
  sourcePrefix: string
  format: string
  status: string
  version?: number
  versionStatus?: string
  rowCount?: number
  createdAt: string
}

export interface DatasetListResponse {
  datasets: DatasetListItem[]
  offset: number
  limit: number
  total: number
}

export interface SchemaResponse {
  columns: SchemaColumn[]
}

export interface PreviewResponse {
  columns: SchemaColumn[]
  rows: Record<string, unknown>[]
  totalRows?: number
  truncated: boolean
  note?: string
}
