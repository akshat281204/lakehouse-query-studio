import { useCallback, useEffect, useRef, useState } from 'react'
import type { ChangeEvent, FormEvent } from 'react'
import { api, ApiError } from '../api/client'
import type {
  DatasetListItem,
  IngestionJob,
  InspectResponse,
} from '../api/types'

interface UploadPageProps {
  onOpenDataset: (id: string) => void
  /** Job polling interval in milliseconds. */
  pollMs?: number
}

const TERMINAL = new Set(['SUCCEEDED', 'FAILED', 'CANCELLED'])

function errorText(err: unknown): string {
  return err instanceof ApiError ? err.message : String(err)
}

function suggestName(filename: string): string {
  return filename
    .replace(/\.[^.]*$/, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 63)
}

export function UploadPage({ onOpenDataset, pollMs = 1500 }: UploadPageProps) {
  const [file, setFile] = useState<File | null>(null)
  const [inspect, setInspect] = useState<InspectResponse | null>(null)
  const [inspecting, setInspecting] = useState(false)
  const [inspectError, setInspectError] = useState<string | null>(null)
  const [name, setName] = useState('')
  const [partitions, setPartitions] = useState<string[]>([])
  const [retain, setRetain] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)
  const [job, setJob] = useState<IngestionJob | null>(null)
  const [pollError, setPollError] = useState<string | null>(null)
  const [retrying, setRetrying] = useState(false)
  const [destination, setDestination] = useState<'new' | 'append'>('new')
  const [target, setTarget] = useState('')
  const [datasets, setDatasets] = useState<DatasetListItem[]>([])
  const jobId = job?.id
  const jobStatus = job?.status
  const timer = useRef<number | undefined>(undefined)

  const poll = useCallback(async (id: string) => {
    try {
      const next = await api.getIngestion(id)
      setJob(next)
      setPollError(null)
    } catch (err) {
      setPollError(errorText(err))
    }
  }, [])

  // Datasets a file can be added to (those with a READY version).
  useEffect(() => {
    api
      .listDatasets({ limit: 100 })
      .then((r) =>
        setDatasets(r.datasets.filter((d) => d.versionStatus === 'READY')),
      )
      .catch(() => setDatasets([]))
  }, [])

  // Poll until the job is terminal. A failed poll keeps retrying; it never
  // changes the displayed job state, so a network blip cannot fake a result.
  useEffect(() => {
    if (!jobId || (jobStatus && TERMINAL.has(jobStatus))) return
    timer.current = window.setTimeout(() => void poll(jobId), pollMs)
    return () => window.clearTimeout(timer.current)
  }, [jobId, jobStatus, job, poll, pollMs])

  const runInspect = async (f: File, appendTo?: string) => {
    setInspecting(true)
    setInspectError(null)
    try {
      setInspect(await api.inspectUpload(f, appendTo))
    } catch (err) {
      setInspect(null)
      setInspectError(errorText(err))
    } finally {
      setInspecting(false)
    }
  }

  const handleFile = async (event: ChangeEvent<HTMLInputElement>) => {
    const picked = event.target.files?.[0] ?? null
    setFile(picked)
    setInspect(null)
    setInspectError(null)
    setPartitions([])
    setJob(null)
    setFormError(null)
    if (!picked) return
    setName(suggestName(picked.name))
    await runInspect(
      picked,
      destination === 'append' && target ? target : undefined,
    )
  }

  // Switching destination or target re-checks the same file against the dataset.
  const chooseDestination = (next: 'new' | 'append') => {
    setDestination(next)
    setFormError(null)
    if (file)
      void runInspect(file, next === 'append' && target ? target : undefined)
  }
  const chooseTarget = (next: string) => {
    setTarget(next)
    setFormError(null)
    if (file) void runInspect(file, next || undefined)
  }

  const togglePartition = (col: string) =>
    setPartitions((prev) =>
      prev.includes(col) ? prev.filter((c) => c !== col) : [...prev, col],
    )

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    if (!file) return
    setSubmitting(true)
    setFormError(null)
    try {
      setJob(
        await api.uploadDataset(file, {
          name: appending ? target : name,
          mode: appending ? 'append' : undefined,
          partitionColumns: isParquet || appending ? undefined : partitions,
          retainOriginal: appending ? undefined : retain,
        }),
      )
    } catch (err) {
      setFormError(errorText(err))
    } finally {
      setSubmitting(false)
    }
  }

  const handleRetry = async () => {
    if (!job) return
    setRetrying(true)
    try {
      setJob(await api.retryIngestion(job.id))
      setPollError(null)
    } catch (err) {
      setPollError(errorText(err))
    } finally {
      setRetrying(false)
    }
  }

  const busy = job !== null && !TERMINAL.has(job.status)
  const isParquet =
    inspect?.format === 'parquet' || /\.parquet$/i.test(file?.name ?? '')
  const appending = destination === 'append'
  const showPartitions = !isParquet && !appending
  const verdict = appending ? inspect?.append : undefined
  const canSubmit = appending
    ? Boolean(target) && verdict?.compatible === true
    : Boolean(name)

  return (
    <main className="page">
      <header className="page-header">
        <div>
          <h1>Upload data</h1>
          <p>
            Load a CSV, JSON, JSONL or Parquet file into a new dataset, or add
            it to one you already have.
          </p>
        </div>
      </header>

      <form className="upload-form card" onSubmit={handleSubmit}>
        <label className="field">
          <span>File (.csv, .json, .jsonl, .parquet)</span>
          <input
            type="file"
            aria-label="Data file"
            accept=".csv,.json,.jsonl,.ndjson,.parquet"
            onChange={(e) => void handleFile(e)}
            disabled={busy}
          />
        </label>

        <fieldset className="destination" disabled={busy}>
          <legend>Where should the data go?</legend>
          <label className="inline-field">
            <input
              type="radio"
              name="destination"
              checked={!appending}
              onChange={() => chooseDestination('new')}
            />
            New dataset
          </label>
          <label className="inline-field">
            <input
              type="radio"
              name="destination"
              checked={appending}
              onChange={() => chooseDestination('append')}
            />
            Add to an existing dataset
          </label>
          {appending ? (
            <select
              aria-label="Existing dataset"
              value={target}
              onChange={(e) => chooseTarget(e.target.value)}
            >
              <option value="">Choose a dataset…</option>
              {datasets.map((d) => (
                <option key={d.id} value={d.name}>
                  {d.name}
                </option>
              ))}
            </select>
          ) : null}
        </fieldset>

        {inspect && appending ? (
          <>
            {appending && target && verdict ? (
              verdict.compatible ? (
                <p className="verdict verdict-ok" role="status">
                  Compatible: adds{' '}
                  <strong>{inspect.rowCount.toLocaleString()}</strong> rows to{' '}
                  <strong>{verdict.dataset}</strong>
                  {verdict.datasetRows !== undefined
                    ? ` (currently ${verdict.datasetRows.toLocaleString()} rows)`
                    : ''}
                  . Earlier versions stay as they are.
                </p>
              ) : (
                <div className="verdict verdict-bad" role="alert">
                  <strong>
                    This file cannot be added to {verdict.dataset}:
                  </strong>
                  <ul>
                    {verdict.issues.map((i) => (
                      <li key={i}>{i}</li>
                    ))}
                  </ul>
                </div>
              )
            ) : null}
            {appending && !target ? (
              <p className="note">Choose the dataset to add this file to.</p>
            ) : null}
          </>
        ) : null}

        {inspecting ? <p className="note">Inspecting file…</p> : null}
        {inspectError ? (
          <p className="error" role="alert">
            {inspectError}
          </p>
        ) : null}

        {inspect ? (
          <>
            <section aria-label="Inferred schema">
              <h2>Inferred schema</h2>
              <p className="note">
                {inspect.rowCount.toLocaleString()} row
                {inspect.rowCount === 1 ? '' : 's'}
                {inspect.sample.length > 0
                  ? `, first ${inspect.sample.length} shown`
                  : ''}
                .{' '}
                {isParquet
                  ? 'A Parquet file is imported exactly as it is; it is not converted.'
                  : 'Empty values become nulls.'}
              </p>
              {inspect.note ? <p className="note">{inspect.note}</p> : null}
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Nullable</th>
                    {showPartitions ? <th>Partition by</th> : null}
                  </tr>
                </thead>
                <tbody>
                  {inspect.columns.map((c) => (
                    <tr key={c.name}>
                      <td>{c.name}</td>
                      <td>{c.type}</td>
                      <td>{c.nullable ? 'yes' : 'no'}</td>
                      {showPartitions ? (
                        <td>
                          <input
                            type="checkbox"
                            aria-label={`Partition by ${c.name}`}
                            checked={partitions.includes(c.name)}
                            disabled={c.type === 'double' || busy}
                            onChange={() => togglePartition(c.name)}
                          />
                        </td>
                      ) : null}
                    </tr>
                  ))}
                </tbody>
              </table>
              {showPartitions ? (
                <p className="note">
                  Partitioning is off by default. Choose only low-cardinality
                  columns: each distinct value becomes its own directory of
                  small files.
                </p>
              ) : null}
            </section>

            <section aria-label="Sample rows">
              <h2>Sample</h2>
              <table className="data-table">
                <thead>
                  <tr>
                    {inspect.columns.map((c) => (
                      <th key={c.name}>{c.name}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {inspect.sample.map((row, i) => (
                    <tr key={i}>
                      {inspect.columns.map((c) => (
                        <td key={c.name}>
                          {row[c.name] === null || row[c.name] === undefined
                            ? '∅'
                            : String(row[c.name])}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </section>

            <div className="register-form">
              {appending ? null : (
                <input
                  aria-label="Dataset name"
                  placeholder="dataset name (lowercase a-z, 0-9, -)"
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  disabled={busy}
                />
              )}
              {appending ? null : (
                <label className="inline-field">
                  <input
                    type="checkbox"
                    checked={retain}
                    onChange={(e) => setRetain(e.target.checked)}
                    disabled={busy}
                  />
                  Keep the original upload
                </label>
              )}
              <button type="submit" disabled={submitting || busy || !canSubmit}>
                {submitting
                  ? 'Uploading…'
                  : appending
                    ? `Add to ${target || 'dataset'}`
                    : isParquet
                      ? 'Import Parquet'
                      : 'Upload and convert'}
              </button>
            </div>
            <p className="note">
              {appending
                ? 'The new rows are added as a new version of the dataset; nothing is copied or rewritten.'
                : 'Using the name of an existing dataset replaces its data with this file as a new version. To keep the existing rows, choose "Add to an existing dataset".'}
            </p>
          </>
        ) : null}

        {formError ? (
          <p className="error" role="alert">
            {formError}
          </p>
        ) : null}
      </form>

      {job ? (
        <section className="job-status" aria-label="Ingestion status">
          <h2>Ingestion</h2>
          <p>
            <span className={`status status-${job.status.toLowerCase()}`}>
              {job.status}
            </span>{' '}
            {job.mode === 'append' ? 'Appending to ' : ''}
            <strong>{job.datasetName}</strong> version {job.version}
            {job.attempt > 1 ? ` (attempt ${job.attempt})` : ''}
          </p>
          {job.status === 'QUEUED' ? (
            <p className="note">Waiting for a worker…</p>
          ) : null}
          {job.status === 'RUNNING' ? (
            <p className="note">
              {job.inputFormat === 'parquet'
                ? 'Importing the Parquet file…'
                : 'Converting to Parquet…'}
            </p>
          ) : null}
          {job.status === 'SUCCEEDED' ? (
            <p>
              <button
                type="button"
                onClick={() => onOpenDataset(job.datasetId)}
              >
                Open dataset
              </button>
            </p>
          ) : null}
          {job.status === 'FAILED' ? (
            <>
              <p className="error" role="alert">
                {job.errorSummary ?? 'Ingestion failed.'}
              </p>
              {job.retryable ? (
                <button type="button" onClick={handleRetry} disabled={retrying}>
                  {retrying ? 'Retrying…' : 'Retry'}
                </button>
              ) : (
                <p className="note">
                  This failure is not retryable. Fix the file and upload it
                  again.
                </p>
              )}
            </>
          ) : null}
          {pollError ? (
            <p className="note" role="status">
              Could not refresh status: {pollError}
            </p>
          ) : null}
        </section>
      ) : null}
    </main>
  )
}
