import { useCallback, useEffect, useRef, useState } from 'react'
import type { ChangeEvent, FormEvent } from 'react'
import { api, ApiError } from '../api/client'
import type { IngestionJob, InspectResponse } from '../api/types'

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

  // Poll until the job is terminal. A failed poll keeps retrying; it never
  // changes the displayed job state, so a network blip cannot fake a result.
  useEffect(() => {
    if (!jobId || (jobStatus && TERMINAL.has(jobStatus))) return
    timer.current = window.setTimeout(() => void poll(jobId), pollMs)
    return () => window.clearTimeout(timer.current)
  }, [jobId, jobStatus, job, poll, pollMs])

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
    setInspecting(true)
    try {
      setInspect(await api.inspectUpload(picked))
    } catch (err) {
      setInspectError(errorText(err))
    } finally {
      setInspecting(false)
    }
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
          name,
          partitionColumns: partitions,
          retainOriginal: retain,
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

  return (
    <main className="page">
      <header className="page-header">
        <h1>Upload data</h1>
        <p>
          Convert a CSV, JSON, or JSONL file into a versioned Parquet dataset.
        </p>
      </header>

      <form className="upload-form" onSubmit={handleSubmit}>
        <label className="field">
          <span>File (.csv, .json, .jsonl)</span>
          <input
            type="file"
            aria-label="Data file"
            accept=".csv,.json,.jsonl,.ndjson"
            onChange={(e) => void handleFile(e)}
            disabled={busy}
          />
        </label>

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
                {inspect.rowCount} row{inspect.rowCount === 1 ? '' : 's'}, first{' '}
                {inspect.sample.length} shown. Empty values become nulls.
              </p>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Nullable</th>
                    <th>Partition by</th>
                  </tr>
                </thead>
                <tbody>
                  {inspect.columns.map((c) => (
                    <tr key={c.name}>
                      <td>{c.name}</td>
                      <td>{c.type}</td>
                      <td>{c.nullable ? 'yes' : 'no'}</td>
                      <td>
                        <input
                          type="checkbox"
                          aria-label={`Partition by ${c.name}`}
                          checked={partitions.includes(c.name)}
                          disabled={c.type === 'double' || busy}
                          onChange={() => togglePartition(c.name)}
                        />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <p className="note">
                Partitioning is off by default. Choose only low-cardinality
                columns: each distinct value becomes its own directory of small
                files.
              </p>
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
              <input
                aria-label="Dataset name"
                placeholder="dataset name (lowercase a-z, 0-9, -)"
                value={name}
                onChange={(e) => setName(e.target.value)}
                disabled={busy}
              />
              <label className="inline-field">
                <input
                  type="checkbox"
                  checked={retain}
                  onChange={(e) => setRetain(e.target.checked)}
                  disabled={busy}
                />
                Keep the original upload
              </label>
              <button type="submit" disabled={submitting || busy || !name}>
                {submitting ? 'Uploading…' : 'Upload and convert'}
              </button>
            </div>
            <p className="note">
              Uploading to an existing dataset name creates a new version.
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
            <strong>{job.datasetName}</strong> version {job.version}
            {job.attempt > 1 ? ` (attempt ${job.attempt})` : ''}
          </p>
          {job.status === 'QUEUED' ? (
            <p className="note">Waiting for a worker…</p>
          ) : null}
          {job.status === 'RUNNING' ? (
            <p className="note">Converting to Parquet…</p>
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
