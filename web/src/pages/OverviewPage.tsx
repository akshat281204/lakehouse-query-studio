import { useEffect, useState } from 'react'
import { api, ApiError } from '../api/client'
import type { IngestionJob, QueryJob, ReadyResponse } from '../api/types'
import { formatDuration } from '../lib/format'

interface Overview {
  ready: ReadyResponse | null
  readyError: string | null
  datasetTotal: number | null
  queries: QueryJob[] | null
  ingestions: IngestionJob[] | null
  errors: string[]
}

const msg = (e: unknown) => (e instanceof ApiError ? e.message : String(e))

export function OverviewPage() {
  const [o, setO] = useState<Overview | null>(null)

  useEffect(() => {
    let cancelled = false
    void (async () => {
      const errors: string[] = []
      const [ready, ds, qs, ing] = await Promise.allSettled([
        api.ready(),
        api.listDatasets({ limit: 1 }),
        api.listQueries(100),
        api.listIngestions(),
      ])
      // /readyz answers 503 with a body when a dependency is down; the client
      // surfaces that as an error, which is shown as "not ready".
      if (qs.status === 'rejected')
        errors.push(`Query history: ${msg(qs.reason)}`)
      if (ds.status === 'rejected') errors.push(`Datasets: ${msg(ds.reason)}`)
      if (ing.status === 'rejected')
        errors.push(`Ingestion jobs: ${msg(ing.reason)}`)
      if (cancelled) return
      setO({
        ready: ready.status === 'fulfilled' ? ready.value : null,
        readyError: ready.status === 'rejected' ? msg(ready.reason) : null,
        datasetTotal: ds.status === 'fulfilled' ? ds.value.total : null,
        queries: qs.status === 'fulfilled' ? qs.value.queries : null,
        ingestions: ing.status === 'fulfilled' ? ing.value.jobs : null,
        errors,
      })
    })()
    return () => {
      cancelled = true
    }
  }, [])

  if (!o) {
    return (
      <main className="page">
        <h1>Overview</h1>
        <p>Loading overview…</p>
      </main>
    )
  }
  const q = o.queries
  const activeQ = q?.filter(
    (j) => j.status === 'QUEUED' || j.status === 'RUNNING',
  ).length
  const failedQ = q?.filter(
    (j) => j.status === 'FAILED' || j.status === 'TIMED_OUT',
  ).length
  const activeI = o.ingestions?.filter(
    (j) => j.status === 'QUEUED' || j.status === 'RUNNING',
  ).length
  const failedI = o.ingestions?.filter((j) => j.status === 'FAILED').length
  const na = '—'

  return (
    <main className="page">
      <header className="page-header">
        <h1>Overview</h1>
      </header>
      {o.errors.map((e) => (
        <p key={e} className="error" role="alert">
          {e}
        </p>
      ))}

      <section aria-label="Dependencies">
        <h2>Dependencies</h2>
        {o.ready ? (
          <ul>
            {Object.entries(o.ready.dependencies).map(([name, d]) => (
              <li key={name}>
                {name}:{' '}
                <span
                  className={`status status-${d.ok ? 'succeeded' : 'failed'}`}
                >
                  {d.ok ? 'ok' : 'down'}
                </span>
                {!d.ok && d.message ? (
                  <span className="muted"> {d.message}</span>
                ) : null}
              </li>
            ))}
          </ul>
        ) : (
          <p className="error">
            Not ready{o.readyError ? `: ${o.readyError}` : ''}
          </p>
        )}
      </section>

      <section aria-label="Summary" className="stat-grid">
        <div>
          <strong data-testid="stat-datasets">{o.datasetTotal ?? na}</strong>{' '}
          datasets
        </div>
        <div>
          <strong data-testid="stat-active-queries">{activeQ ?? na}</strong>{' '}
          active queries
        </div>
        <div>
          <strong data-testid="stat-failed-queries">{failedQ ?? na}</strong>{' '}
          failed queries
        </div>
        <div>
          <strong data-testid="stat-active-ingestions">{activeI ?? na}</strong>{' '}
          active ingestions
        </div>
        <div>
          <strong data-testid="stat-failed-ingestions">{failedI ?? na}</strong>{' '}
          failed ingestions
        </div>
      </section>
      <p className="note">
        Job counts cover the most recent jobs the API returns (up to 100
        queries).
      </p>

      <section aria-label="Recent queries">
        <h2>Recent queries</h2>
        {q && q.length === 0 ? <p>No queries yet.</p> : null}
        {q && q.length > 0 ? (
          <ul>
            {q.slice(0, 5).map((j) => (
              <li key={j.id}>
                <code className="statement">
                  {j.statement.length > 80
                    ? `${j.statement.slice(0, 80)}…`
                    : j.statement}
                </code>{' '}
                <span
                  className={`status status-${j.status.toLowerCase().replace('_', '-')}`}
                >
                  {j.status}
                </span>{' '}
                <span className="muted">{formatDuration(j.durationMs)}</span>
              </li>
            ))}
          </ul>
        ) : null}
      </section>
    </main>
  )
}
