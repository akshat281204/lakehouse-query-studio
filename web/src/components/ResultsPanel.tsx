import { useCallback, useEffect, useState } from 'react'
import { api, ApiError } from '../api/client'
import type { QueryJob, QueryResults } from '../api/types'
import { formatBytes, formatDuration } from '../lib/format'
import { Chart } from './Chart'

const PAGE = 100

interface ResultsPanelProps {
  job: QueryJob
}

export function ResultsPanel({ job }: ResultsPanelProps) {
  const [offset, setOffset] = useState(0)
  const [res, setRes] = useState<QueryResults | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [tab, setTab] = useState<'table' | 'chart' | 'details'>('table')

  const load = useCallback(
    async (off: number) => {
      setError(null)
      try {
        setRes(await api.getQueryResults(job.id, off, PAGE))
        setOffset(off)
      } catch (e) {
        setError(e instanceof ApiError ? e.message : String(e))
      }
    },
    [job.id],
  )

  useEffect(() => {
    setRes(null)
    void load(0)
  }, [load])

  return (
    <section className="results" aria-label="Results">
      <div className="tabs" role="tablist">
        {(['table', 'chart', 'details'] as const).map((t) => (
          <button
            key={t}
            role="tab"
            type="button"
            aria-selected={tab === t}
            onClick={() => setTab(t)}
          >
            {t === 'table' ? 'Results' : t === 'chart' ? 'Chart' : 'Details'}
          </button>
        ))}
        <a className="button-link" href={api.queryExportUrl(job.id)} download>
          Export CSV
        </a>
      </div>

      {error ? (
        <p className="error" role="alert">
          {error}
        </p>
      ) : null}
      {!error && !res ? <p>Loading results…</p> : null}

      {res && tab === 'table' ? (
        <>
          <p data-testid="result-summary">
            {res.totalRows.toLocaleString()} row{res.totalRows === 1 ? '' : 's'}{' '}
            × {res.columns.length} column
            {res.columns.length === 1 ? '' : 's'}
            {res.truncated ? ' (result truncated at the server limit)' : ''}
          </p>
          {res.totalRows === 0 ? (
            <p>The query returned no rows.</p>
          ) : (
            <div className="table-scroll">
              <table className="data-table">
                <thead>
                  <tr>
                    {res.columns.map((c, i) => (
                      <th key={i} title={c.type}>
                        {c.name}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {res.rows.map((r, ri) => (
                    <tr key={ri}>
                      {r.map((v, ci) => (
                        <td key={ci}>
                          {v === null ? (
                            <span className="muted">null</span>
                          ) : (
                            String(v)
                          )}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <div className="pager">
            <button
              type="button"
              disabled={offset === 0}
              onClick={() => void load(Math.max(0, offset - PAGE))}
            >
              Previous
            </button>
            <span>
              Rows {res.totalRows === 0 ? 0 : offset + 1}–
              {offset + res.returned}
            </span>
            <button
              type="button"
              disabled={res.nextOffset === null}
              onClick={() => void load(res.nextOffset ?? 0)}
            >
              Next
            </button>
          </div>
        </>
      ) : null}

      {res && tab === 'chart' ? (
        <Chart columns={res.columns} rows={res.rows} />
      ) : null}

      {tab === 'details' ? (
        <dl className="dataset-info">
          <dt>Status</dt>
          <dd>{job.status}</dd>
          <dt>Duration</dt>
          <dd>{formatDuration(job.durationMs)}</dd>
          <dt>Rows returned</dt>
          <dd>
            {job.rowsReturned ?? '—'}
            {job.truncated ? ' (truncated)' : ''}
          </dd>
          <dt>Data staged</dt>
          <dd>{formatBytes(job.bytesStaged)}</dd>
          <dt>Result size</dt>
          <dd>{formatBytes(job.resultBytes)}</dd>
          <dt>Attempt</dt>
          <dd>{job.attempt}</dd>
          <dt>Execution plan</dt>
          <dd>
            {job.planSummary ? (
              <pre className="plan">{job.planSummary}</pre>
            ) : (
              'Not available'
            )}
          </dd>
        </dl>
      ) : null}
    </section>
  )
}
