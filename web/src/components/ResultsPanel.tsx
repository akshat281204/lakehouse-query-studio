import { useCallback, useEffect, useState } from 'react'
import { api, ApiError } from '../api/client'
import type { CellValue, QueryJob, QueryResults } from '../api/types'
import { formatBytes, formatDuration } from '../lib/format'
import { Chart } from './Chart'

const PAGE = 100

const MAX_DECIMALS = 4
const formatters = new Map<number, Intl.NumberFormat>()
function formatterFor(decimals: number): Intl.NumberFormat {
  let f = formatters.get(decimals)
  if (!f) {
    f = new Intl.NumberFormat(undefined, {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    })
    formatters.set(decimals, f)
  }
  return f
}

/** Decimals needed to show every value on this page of a double column
 * (capped), so a column lines up: 200.93, 57.15, 1,902.90. */
function columnDecimals(rows: CellValue[][], col: number): number {
  let max = 0
  for (const r of rows) {
    const v = r[col]
    if (typeof v !== 'number' || !Number.isFinite(v)) continue
    const fixed = Number(v.toFixed(MAX_DECIMALS)).toString()
    const dot = fixed.indexOf('.')
    if (dot >= 0) max = Math.max(max, fixed.length - dot - 1)
  }
  return max
}

/** Display form of a cell. Doubles are tidied (no float noise); integers and
 * text are shown exactly. The CSV export and the stored result are unchanged. */
function displayCell(v: CellValue, type: string, decimals: number): string {
  if (typeof v === 'number' && type === 'double') {
    return formatterFor(decimals).format(v)
  }
  return String(v)
}

const isNumericType = (t: string) => t === 'int64' || t === 'double'

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

  const decimals = res
    ? res.columns.map((_, i) => columnDecimals(res.rows, i))
    : []

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
          <div className="result-bar">
            <p data-testid="result-summary" className="result-summary">
              {res.totalRows.toLocaleString()} row
              {res.totalRows === 1 ? '' : 's'} × {res.columns.length} column
              {res.columns.length === 1 ? '' : 's'}
              {res.truncated ? ' (result truncated at the server limit)' : ''}
            </p>
            <ul className="result-meta" aria-label="Run details">
              {job.durationMs !== undefined ? (
                <li>
                  <b>{formatDuration(job.durationMs)}</b> run time
                </li>
              ) : null}
              {job.bytesStaged ? (
                <li>
                  <b>{formatBytes(job.bytesStaged)}</b> read
                </li>
              ) : null}
              {job.resultBytes ? (
                <li>
                  <b>{formatBytes(job.resultBytes)}</b> result
                </li>
              ) : null}
            </ul>
          </div>
          {res.totalRows === 0 ? (
            <p>The query returned no rows.</p>
          ) : (
            <div className="table-scroll result-grid">
              <table className="data-table">
                <thead>
                  <tr>
                    <th className="rownum" aria-label="Row number">
                      #
                    </th>
                    {res.columns.map((c, i) => (
                      <th
                        key={i}
                        title={c.type}
                        className={isNumericType(c.type) ? 'num' : undefined}
                      >
                        <span className="col-name">{c.name}</span>
                        <span className="col-type">{c.type}</span>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {res.rows.map((r, ri) => (
                    <tr key={ri}>
                      <td className="rownum">{offset + ri + 1}</td>
                      {r.map((v, ci) => (
                        <td
                          key={ci}
                          className={
                            isNumericType(res.columns[ci].type)
                              ? 'num'
                              : undefined
                          }
                          title={v === null ? undefined : String(v)}
                        >
                          {v === null ? (
                            <span className="muted">null</span>
                          ) : (
                            displayCell(v, res.columns[ci].type, decimals[ci])
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
