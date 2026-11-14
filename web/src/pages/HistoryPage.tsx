import { useEffect, useState } from 'react'
import { api, ApiError } from '../api/client'
import type { QueryJob } from '../api/types'
import { formatDuration, timeAgo } from '../lib/format'

interface HistoryPageProps {
  onRerun: (statement: string) => void
}

export function HistoryPage({ onRerun }: HistoryPageProps) {
  const [jobs, setJobs] = useState<QueryJob[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [filter, setFilter] = useState<string>('ALL')
  const [search, setSearch] = useState('')

  useEffect(() => {
    api
      .listQueries(50)
      .then((r) => setJobs(r.queries))
      .catch((e: unknown) =>
        setError(e instanceof ApiError ? e.message : String(e)),
      )
  }, [])

  const FILTERS: { key: string; label: string }[] = [
    { key: 'ALL', label: 'All' },
    { key: 'SUCCEEDED', label: 'Succeeded' },
    { key: 'FAILED', label: 'Failed' },
    { key: 'CANCELLED', label: 'Cancelled' },
    { key: 'ACTIVE', label: 'Active' },
  ]
  const visible = (jobs ?? []).filter((j) => {
    if (filter === 'ACTIVE' && !['QUEUED', 'RUNNING'].includes(j.status))
      return false
    if (filter === 'FAILED' && !['FAILED', 'TIMED_OUT'].includes(j.status))
      return false
    if (!['ALL', 'ACTIVE', 'FAILED'].includes(filter) && j.status !== filter)
      return false
    return !search || j.statement.toLowerCase().includes(search.toLowerCase())
  })

  return (
    <main className="page">
      <header className="page-header">
        <div>
          <h1>Query history</h1>
          <p>Recent queries with status and duration</p>
        </div>
      </header>
      {error ? (
        <p className="error" role="alert">
          {error}
        </p>
      ) : null}
      {!error && jobs === null ? <p>Loading history…</p> : null}
      {jobs && jobs.length === 0 ? (
        <p className="empty">No queries yet. Run one from the SQL workbench.</p>
      ) : null}
      {jobs && jobs.length > 0 ? (
        <div className="filter-bar">
          <div className="segmented" role="group" aria-label="Status filter">
            {FILTERS.map((f) => (
              <button
                key={f.key}
                type="button"
                aria-pressed={filter === f.key}
                onClick={() => setFilter(f.key)}
              >
                {f.label}
              </button>
            ))}
          </div>
          <input
            aria-label="Search statements"
            className="search-input inline"
            placeholder="Search statements"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
          <span className="muted">
            {visible.length} of {jobs.length}
          </span>
        </div>
      ) : null}
      {jobs && jobs.length > 0 && visible.length === 0 ? (
        <p className="empty">No queries match this filter.</p>
      ) : null}
      {jobs && visible.length > 0 ? (
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
            {visible.map((j) => (
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
                    {j.status.replace('_', ' ')}
                  </span>
                </td>
                <td>{formatDuration(j.durationMs)}</td>
                <td>{j.rowsReturned ?? '—'}</td>
                <td title={new Date(j.createdAt).toLocaleString()}>
                  {timeAgo(j.createdAt)}
                </td>
                <td>
                  {/* Rerun re-submits through the normal validated path as a new job. */}
                  <button
                    type="button"
                    className="btn-small"
                    onClick={() => onRerun(j.statement)}
                  >
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
