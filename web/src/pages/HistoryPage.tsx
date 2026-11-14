import { useEffect, useState } from 'react'
import { api, ApiError } from '../api/client'
import type { QueryJob } from '../api/types'
import { formatDuration } from '../lib/format'

interface HistoryPageProps {
  onRerun: (statement: string) => void
}

export function HistoryPage({ onRerun }: HistoryPageProps) {
  const [jobs, setJobs] = useState<QueryJob[] | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    api
      .listQueries(50)
      .then((r) => setJobs(r.queries))
      .catch((e: unknown) =>
        setError(e instanceof ApiError ? e.message : String(e)),
      )
  }, [])

  return (
    <main className="page">
      <header className="page-header">
        <h1>Query history</h1>
      </header>
      {error ? (
        <p className="error" role="alert">
          {error}
        </p>
      ) : null}
      {!error && jobs === null ? <p>Loading history…</p> : null}
      {jobs && jobs.length === 0 ? (
        <p>No queries yet. Run one from the SQL workbench.</p>
      ) : null}
      {jobs && jobs.length > 0 ? (
        <table className="data-table">
          <thead>
            <tr>
              <th>Statement</th>
              <th>Status</th>
              <th>Duration</th>
              <th>Rows</th>
              <th>Submitted</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {jobs.map((j) => (
              <tr key={j.id}>
                <td>
                  <code className="statement">
                    {j.statement.length > 120
                      ? `${j.statement.slice(0, 120)}…`
                      : j.statement}
                  </code>
                </td>
                <td>
                  <span
                    className={`status status-${j.status.toLowerCase().replace('_', '-')}`}
                  >
                    {j.status}
                  </span>
                </td>
                <td>{formatDuration(j.durationMs)}</td>
                <td>{j.rowsReturned ?? '—'}</td>
                <td>{new Date(j.createdAt).toLocaleString()}</td>
                <td>
                  {/* Rerun re-submits through the normal validated path as a new job. */}
                  <button type="button" onClick={() => onRerun(j.statement)}>
                    Rerun
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      ) : null}
    </main>
  )
}
