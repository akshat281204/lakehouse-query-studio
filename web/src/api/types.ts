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

export type IngestionStatus =
  | 'QUEUED'
  | 'RUNNING'
  | 'SUCCEEDED'
  | 'FAILED'
  | 'CANCELLED'

export interface IngestionJob {
  id: string
  datasetId: string
  datasetName: string
  versionId: string
  version: number
  kind: string
  status: IngestionStatus
  inputFormat: string
  partitionColumns: string[]
  failureReason?: string
  errorSummary?: string
  retryable: boolean
  retryOf?: string
  attempt: number
  createdAt: string
  startedAt?: string
  finishedAt?: string
}

export interface IngestionListResponse {
  jobs: IngestionJob[]
}

export interface InspectColumn {
  name: string
  type: string
  nullable: boolean
}

export interface InspectResponse {
  format: string
  columns: InspectColumn[]
  rowCount: number
  sample: Record<string, unknown>[]
}

export type QueryStatus =
  | 'QUEUED'
  | 'RUNNING'
  | 'SUCCEEDED'
  | 'FAILED'
  | 'CANCELLED'
  | 'TIMED_OUT'

export interface QueryJob {
  id: string
  statement: string
  statementHash: string
  datasetVersionIds: string[]
  status: QueryStatus
  failureReason?: string
  errorSummary?: string
  rowsReturned?: number
  truncated: boolean
  resultBytes?: number
  bytesStaged?: number
  planSummary?: string
  durationMs?: number
  attempt: number
  cancelRequested: boolean
  createdAt: string
  startedAt?: string
  finishedAt?: string
}

export interface QueryListResponse {
  queries: QueryJob[]
  offset: number
  limit: number
}

export interface ResultColumn {
  name: string
  type: string
}

export type CellValue = string | number | boolean | null

export interface QueryResults {
  columns: ResultColumn[]
  rows: CellValue[][]
  offset: number
  returned: number
  nextOffset: number | null
  totalRows: number
  truncated: boolean
}
