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

export interface QueryTotals {
  total: number
  succeeded: number
  failed: number
  cancelled: number
  timedOut: number
  queued: number
  running: number
  rowsReturned: number
  bytesStaged: number
  avgMs: number
  p50Ms: number
  p95Ms: number
  p99Ms: number
  maxMs: number
}

export interface TimelineBucket {
  start: string
  succeeded: number
  failed: number
  cancelled: number
  p50Ms: number
}

export interface HistogramBucket {
  /** Upper bound in ms; 0 means "slower than every bound". */
  leMs: number
  count: number
}

export interface ReasonCount {
  reason: string
  count: number
}

export interface Summary {
  windowSeconds: number
  generatedAt: string
  queries: QueryTotals
  timeline: TimelineBucket[]
  latencyHistogram: HistogramBucket[]
  failureReasons: ReasonCount[]
  ingestions: {
    total: number
    succeeded: number
    failed: number
    active: number
  }
  datasets: number
  truncated: boolean
}

export type StatsWindow = '1h' | '6h' | '24h' | '7d'

export interface SavedQuery {
  id: string
  name: string
  description: string
  statement: string
  createdAt: string
  updatedAt: string
}

export interface SavedQueryInput {
  name: string
  description?: string
  statement: string
}

export interface SavedQueryListResponse {
  savedQueries: SavedQuery[]
  total: number
  offset: number
  limit: number
}

export interface ProfileColumn {
  name: string
  type: string
  nullable: boolean
  isPartition: boolean
  /** true only when every file supplied min/max for this column */
  hasRange: boolean
  min?: string
  max?: string
  nullCount?: number
  truncated?: boolean
}

export interface DatasetProfile {
  rows: number
  files: number
  columns: ProfileColumn[]
  note?: string
}
