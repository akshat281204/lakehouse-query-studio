import { useCallback, useEffect, useState } from 'react'
import { api, ApiError } from '../api/client'
import type {
  IngestionJob,
  QueryJob,
  ReadyResponse,
  StatsWindow,
  Summary,
} from '../api/types'
import {
  ActivityChart,
  Donut,
  LatencyHistogram,
  RangeBar,
} from '../components/charts'
import { compact, formatBytesLong, ms, timeAgo } from '../lib/format'

const WINDOWS: { key: StatsWindow; label: string }[] = [
  { key: '1h', label: '1h' },
  { key: '6h', label: '6h' },
  { key: '24h', label: '24h' },
  { key: '7d', label: '7d' },
]

const DEP_LABELS: Record<string, string> = {
  postgres: 'PostgreSQL',
  objectstore: 'Object store',
}

const REASON_LABELS: Record<string, string> = {
  engine_error: 'Engine error (bad SQL or data)',
  object_store: 'Object store unavailable',
  resource_exceeded: 'Resource limit exceeded',
  query_denied: 'Denied by safety checks',
  worker_interrupted: 'Worker interrupted',
  internal: 'Internal error',
  timeout: 'Timed out',
}

const REFRESH_MS = 15_000

interface Data {
  summary: Summary | null
  ready: ReadyResponse | null
  readyError: string | null
  recent: QueryJob[] | null
  ingestions: IngestionJob[] | null
  errors: string[]
  at: number
}

const msg = (e: unknown) => (e instanceof ApiError ? e.message : String(e))
const statusClass = (s: string) =>
  `status status-${s.toLowerCase().replace('_', '-')}`

export function OverviewPage() {
  const [win, setWin] = useState<StatsWindow>('24h')
  const [d, setD] = useState<Data | null>(null)
  const [loading, setLoading] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    const [stats, ready, recent, ing] = await Promise.allSettled([
      api.getStats(win),
      api.ready(),
      api.listQueries(8),
      api.listIngestions(),
    ])
    const errors: string[] = []
    if (stats.status === 'rejected')
      errors.push(`Statistics: ${msg(stats.reason)}`)
    if (recent.status === 'rejected')
      errors.push(`Query history: ${msg(recent.reason)}`)
    if (ing.status === 'rejected')
      errors.push(`Ingestion jobs: ${msg(ing.reason)}`)
    setD({
      summary: stats.status === 'fulfilled' ? stats.value : null,
      ready: ready.status === 'fulfilled' ? ready.value : null,
      readyError: ready.status === 'rejected' ? msg(ready.reason) : null,
      recent: recent.status === 'fulfilled' ? recent.value.queries : null,
      ingestions: ing.status === 'fulfilled' ? ing.value.jobs : null,
      errors,
      at: Date.now(),
    })
    setLoading(false)
  }, [win])

  useEffect(() => {
    void load()
    const t = setInterval(() => void load(), REFRESH_MS)
    return () => clearInterval(t)
  }, [load])

  const header = (
    <header className="page-header">
      <div>
        <h1>Overview</h1>
        <p>
          <span className="live" aria-hidden="true" />
          {d
            ? `Live · updated ${new Date(d.at).toLocaleTimeString()}`
            : 'Connecting…'}
        </p>
      </div>
      <div className="header-actions">
        <div className="segmented" role="group" aria-label="Time window">
          {WINDOWS.map((w) => (
            <button
              key={w.key}
              type="button"
              aria-pressed={win === w.key}
              onClick={() => setWin(w.key)}
            >
              {w.label}
            </button>
          ))}
        </div>
        <button type="button" onClick={() => void load()} disabled={loading}>
          {loading ? 'Refreshing…' : 'Refresh'}
        </button>
      </div>
    </header>
  )

  if (!d) {
    return (
      <main className="page">
        {header}
        <p>Loading overview…</p>
      </main>
    )
  }

  const s = d.summary
  const q = s?.queries
  const finished = q ? q.succeeded + q.failed + q.timedOut : 0
  const successRate = q && finished > 0 ? (q.succeeded / finished) * 100 : null
  const na = '—'
  const winLabel = WINDOWS.find((w) => w.key === win)?.label ?? win
  const maxReason = Math.max(
    1,
    ...(s?.failureReasons.map((r) => r.count) ?? [1]),
  )
  const failedTotal = q ? q.failed + q.timedOut : 0

  const healthy = d.ready
    ? Object.values(d.ready.dependencies).every((x) => x.ok)
    : false
  const rate =
    successRate === null
      ? na
      : `${successRate.toFixed(successRate === 100 ? 0 : 1)}%`
  const rateTone =
    successRate === null
      ? ''
      : successRate >= 95
        ? 'tone-ok'
        : successRate >= 80
          ? 'tone-warn'
          : 'tone-err'

  return (
    <main className="page overview">
      {header}

      {d.errors.map((e) => (
        <p key={e} className="error" role="alert">
          {e}
        </p>
      ))}

      <section aria-label="Key metrics" className="hero">
        <p className="hero-sentence">
          <strong data-testid="kpi-queries">{q ? compact(q.total) : na}</strong>{' '}
          queries in the last {winLabel}.{' '}
          <strong data-testid="kpi-success-rate" className={rateTone}>
            {rate}
          </strong>{' '}
          succeeded, with a median of{' '}
          <strong data-testid="kpi-p50">
            {q && q.succeeded > 0 ? ms(q.p50Ms) : na}
          </strong>{' '}
          and a p95 of{' '}
          <strong data-testid="kpi-p95">
            {q && q.succeeded > 0 ? ms(q.p95Ms) : na}
          </strong>
          .
        </p>
        <dl className="figures">
          <div>
            <dt>Rows returned</dt>
            <dd data-testid="kpi-rows">{q ? compact(q.rowsReturned) : na}</dd>
            <span>last {winLabel}</span>
          </div>
          <div>
            <dt>Data staged</dt>
            <dd data-testid="kpi-staged">
              {q ? formatBytesLong(q.bytesStaged) : na}
            </dd>
            <span>read from object store</span>
          </div>
          <div>
            <dt>Datasets</dt>
            <dd data-testid="kpi-datasets">{s ? String(s.datasets) : na}</dd>
            <span>active</span>
          </div>
          <div>
            <dt>In flight</dt>
            <dd data-testid="kpi-queue">
              {q ? String(q.queued + q.running) : na}
            </dd>
            <span>{q ? `${q.running} running · ${q.queued} queued` : ' '}</span>
          </div>
          <div className="figure-range">
            <dt>Latency spread</dt>
            <dd className="range-dd">
              {q && q.succeeded > 0 ? (
                <RangeBar p50={q.p50Ms} p95={q.p95Ms} max={q.maxMs} />
              ) : (
                na
              )}
            </dd>
            <span>
              {q && q.succeeded > 0
                ? `median ${ms(q.p50Ms)} · max ${ms(q.maxMs)}`
                : ' '}
            </span>
          </div>
        </dl>
      </section>

      <section className="sheet activity" aria-label="Query activity">
        <div className="sheet-head">
          <h2>Query activity</h2>
          <div className="legend">
            <span>
              <i className="sw bar-ok" /> Succeeded
            </span>
            <span>
              <i className="sw bar-err" /> Failed
            </span>
            <span>
              <i className="sw bar-warn" /> Cancelled
            </span>
          </div>
        </div>
        {s ? (
          <ActivityChart
            timeline={s.timeline}
            windowSeconds={s.windowSeconds}
          />
        ) : (
          <p className="muted">Unavailable</p>
        )}
        {s?.truncated ? (
          <p className="note">
            Showing the most recent 50,000 queries in this window.
          </p>
        ) : null}
      </section>

      <div className="split">
        <section className="sheet" aria-label="Latency distribution">
          <div className="sheet-head">
            <h2>Latency distribution</h2>
            <span className="muted">succeeded queries, last {winLabel}</span>
          </div>
          {s ? (
            <LatencyHistogram buckets={s.latencyHistogram} />
          ) : (
            <p className="muted">Unavailable</p>
          )}
        </section>

        <section className="sheet" aria-label="Outcomes">
          <div className="sheet-head">
            <h2>Outcomes</h2>
          </div>
          {q ? (
            <div className="donut-row">
              <Donut
                segments={[
                  {
                    label: 'Succeeded',
                    value: q.succeeded,
                    className: 'seg-ok',
                  },
                  {
                    label: 'Failed',
                    value: q.failed + q.timedOut,
                    className: 'seg-err',
                  },
                  {
                    label: 'Cancelled',
                    value: q.cancelled,
                    className: 'seg-warn',
                  },
                  {
                    label: 'In flight',
                    value: q.queued + q.running,
                    className: 'seg-info',
                  },
                ]}
              />
              <ul className="legend-list">
                <li>
                  <i className="sw bar-ok" /> Succeeded <b>{q.succeeded}</b>
                </li>
                <li>
                  <i className="sw bar-err" /> Failed <b>{q.failed}</b>
                </li>
                <li>
                  <i className="sw bar-err" /> Timed out <b>{q.timedOut}</b>
                </li>
                <li>
                  <i className="sw bar-warn" /> Cancelled <b>{q.cancelled}</b>
                </li>
                <li>
                  <i className="sw bar-info" /> In flight{' '}
                  <b>{q.queued + q.running}</b>
                </li>
              </ul>
            </div>
          ) : (
            <p className="muted">Unavailable</p>
          )}
        </section>
      </div>

      <div className="triple">
        <section className="sheet" aria-label="System health">
          <div className="sheet-head">
            <h2>System health</h2>
            {d.ready ? (
              <span className={healthy ? 'pill-ok' : 'pill-bad'}>
                {healthy ? 'all ready' : 'degraded'}
              </span>
            ) : null}
          </div>
          {d.ready ? (
            <ul className="health-list">
              {Object.entries(d.ready.dependencies).map(([name, dep]) => (
                <li key={name}>
                  <span className={`dot ${dep.ok ? 'dot-ok' : 'dot-bad'}`} />
                  <span className="health-name">
                    {DEP_LABELS[name] ?? name}
                  </span>
                  <span
                    className={
                      dep.ok
                        ? 'status status-succeeded'
                        : 'status status-failed'
                    }
                  >
                    {dep.ok ? 'ok' : 'down'}
                  </span>
                  {!dep.ok && dep.message ? (
                    <span className="muted health-msg">{dep.message}</span>
                  ) : null}
                </li>
              ))}
              <li>
                <span className="dot dot-ok" />
                <span className="health-name">Query engine</span>
                <span className="muted">single-node DuckDB</span>
              </li>
            </ul>
          ) : (
            <p className="error">
              Not ready{d.readyError ? `: ${d.readyError}` : ''}
            </p>
          )}
          {s ? (
            <div className="ingest-summary" data-testid="ingest-summary">
              <h3>Ingestion, last {winLabel}</h3>
              <div className="mini-stats">
                <span>
                  <b>{s.ingestions.total}</b> jobs
                </span>
                <span>
                  <b>{s.ingestions.succeeded}</b> succeeded
                </span>
                <span>
                  <b>{s.ingestions.failed}</b> failed
                </span>
                <span>
                  <b>{s.ingestions.active}</b> active
                </span>
              </div>
            </div>
          ) : null}
        </section>

        <section className="sheet" aria-label="Failure reasons">
          <div className="sheet-head">
            <h2>Why queries fail</h2>
            <span className="muted">{failedTotal} failed</span>
          </div>
          {s && s.failureReasons.length === 0 ? (
            <p className="muted">No failures in this window.</p>
          ) : null}
          <ul className="reason-list">
            {s?.failureReasons.map((r) => (
              <li key={r.reason}>
                <div className="reason-row">
                  <span>{REASON_LABELS[r.reason] ?? r.reason}</span>
                  <b>{r.count}</b>
                </div>
                <div className="reason-bar">
                  <span style={{ width: `${(r.count / maxReason) * 100}%` }} />
                </div>
              </li>
            ))}
          </ul>
        </section>

        <section className="sheet" aria-label="Recent ingestions">
          <div className="sheet-head">
            <h2>Recent ingestions</h2>
            <a href="#/upload">Upload</a>
          </div>
          {d.ingestions && d.ingestions.length === 0 ? (
            <p className="muted">No uploads yet.</p>
          ) : null}
          <ul className="activity-list">
            {d.ingestions?.slice(0, 6).map((j) => (
              <li key={j.id}>
                <div>
                  <strong>{j.datasetName}</strong>
                  <span className="muted"> v{j.version}</span>
                  <div className="muted">{timeAgo(j.createdAt, d.at)}</div>
                </div>
                <span className={statusClass(j.status)}>{j.status}</span>
              </li>
            ))}
          </ul>
        </section>
      </div>

      <section className="sheet" aria-label="Recent queries">
        <div className="sheet-head">
          <h2>Recent queries</h2>
          <a href="#/history">View all</a>
        </div>
        {d.recent && d.recent.length === 0 ? (
          <p className="empty">
            No queries yet. Run one from the SQL workbench.
          </p>
        ) : null}
        {d.recent && d.recent.length > 0 ? (
          <table className="data-table flat">
            <thead>
              <tr>
                <th>Statement</th>
                <th>Status</th>
                <th className="num">Duration</th>
                <th className="num">Rows</th>
                <th className="num">When</th>
              </tr>
            </thead>
            <tbody>
              {d.recent.map((j) => (
                <tr key={j.id}>
                  <td>
                    <code className="statement">
                      {j.statement.length > 110
                        ? `${j.statement.slice(0, 110)}…`
                        : j.statement}
                    </code>
                  </td>
                  <td>
                    <span className={statusClass(j.status)}>
                      {j.status.replace('_', ' ')}
                    </span>
                  </td>
                  <td className="num">
                    {j.durationMs === undefined ? '—' : ms(j.durationMs)}
                  </td>
                  <td className="num">{j.rowsReturned ?? '—'}</td>
                  <td className="num">{timeAgo(j.createdAt, d.at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : null}
      </section>

      <p className="note foot">
        Updated {new Date(d.at).toLocaleTimeString()} · refreshes every 15 s ·
        figures come from the durable job records, not estimates.
      </p>
    </main>
  )
}
