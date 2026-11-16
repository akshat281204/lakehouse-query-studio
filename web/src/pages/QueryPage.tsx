import { useCallback, useEffect, useRef, useState } from 'react'
import { api, ApiError } from '../api/client'
import type { QueryJob } from '../api/types'
import { ResultsPanel } from '../components/ResultsPanel'
import { SchemaBrowser } from '../components/SchemaBrowser'
import { SqlEditor } from '../components/SqlEditor'
import { formatDuration, isTerminal } from '../lib/format'

const DEFAULT_SQL = 'SELECT 1 AS one'

interface QueryPageProps {
  initialStatement?: string
  autoRun?: boolean
  pollMs?: number
}

export function QueryPage({
  initialStatement,
  autoRun,
  pollMs = 500,
}: QueryPageProps) {
  const [sql, setSql] = useState(initialStatement ?? DEFAULT_SQL)
  const [job, setJob] = useState<QueryJob | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [elapsed, setElapsed] = useState(0)
  const runId = useRef(0)
  const alive = useRef(true)

  // Bumping the counter makes any in-flight polling loop stop on its next turn.
  const invalidateRuns = () => {
    runId.current++
  }

  useEffect(() => {
    alive.current = true
    return () => {
      alive.current = false
      invalidateRuns()
    }
  }, [])

  const run = useCallback(
    async (statement: string) => {
      const mine = ++runId.current
      setError(null)
      setJob(null)
      setSubmitting(true)
      const started = Date.now()
      setElapsed(0)
      try {
        let current = await api.submitQuery(statement)
        if (mine !== runId.current) return
        setJob(current)
        while (!isTerminal(current.status)) {
          await new Promise((r) => setTimeout(r, pollMs))
          if (mine !== runId.current) return
          setElapsed(Date.now() - started)
          current = await api.getQuery(current.id)
          if (mine !== runId.current) return
          setJob(current)
        }
        setElapsed(Date.now() - started)
      } catch (e) {
        if (mine === runId.current)
          setError(e instanceof ApiError ? e.message : String(e))
      } finally {
        if (mine === runId.current && alive.current) setSubmitting(false)
      }
    },
    [pollMs],
  )

  const started = useRef(false)
  useEffect(() => {
    if (autoRun && initialStatement && !started.current) {
      started.current = true
      void run(initialStatement)
    }
  }, [autoRun, initialStatement, run])

  const cancel = async () => {
    if (!job) return
    try {
      setJob(await api.cancelQuery(job.id))
    } catch (e) {
      setError(e instanceof ApiError ? e.message : String(e))
    }
  }

  const active = job !== null && !isTerminal(job.status)

  return (
    <main className="page query-page">
      <header className="page-header">
        <h1>SQL workbench</h1>
      </header>
      <div className="workbench">
        <SchemaBrowser
          onInsert={(t) =>
            setSql((s) => `${s}${s && !/\s$/.test(s) ? ' ' : ''}${t}`)
          }
        />
        <div className="workbench-main">
          <SqlEditor
            value={sql}
            onChange={setSql}
            onRun={() => !submitting && void run(sql)}
            disabled={submitting}
          />
          <div className="toolbar">
            <button
              type="button"
              onClick={() => void run(sql)}
              disabled={submitting || !sql.trim()}
            >
              Run
            </button>
            <button
              type="button"
              onClick={() => void cancel()}
              disabled={!active || job?.cancelRequested}
            >
              {job?.cancelRequested && active ? 'Cancelling…' : 'Cancel'}
            </button>
            {job ? (
              <span
                className={`status status-${job.status.toLowerCase().replace('_', '-')}`}
                data-testid="query-status"
              >
                {job.status}
              </span>
            ) : null}
            {submitting || job ? (
              <span className="muted" data-testid="elapsed">
                {formatDuration(job?.durationMs ?? elapsed)}
              </span>
            ) : null}
            <span className="muted">Ctrl/⌘+Enter runs</span>
          </div>

          {error ? (
            <p className="error" role="alert">
              {error}
            </p>
          ) : null}
          {job && (job.status === 'FAILED' || job.status === 'TIMED_OUT') ? (
            <p className="error" role="alert">
              {job.status === 'TIMED_OUT'
                ? 'The query exceeded the execution timeout.'
                : 'The query failed'}
              {job.failureReason ? ` (${job.failureReason})` : ''}
              {job.errorSummary ? `: ${job.errorSummary}` : ''}
            </p>
          ) : null}
          {job?.status === 'CANCELLED' ? (
            <p className="note" role="status">
              The query was cancelled.
            </p>
          ) : null}
          {job?.status === 'QUEUED' ? (
            <p role="status">Queued — waiting for a worker…</p>
          ) : null}
          {job?.status === 'RUNNING' ? <p role="status">Running…</p> : null}
          {job?.status === 'SUCCEEDED' ? <ResultsPanel job={job} /> : null}
        </div>
      </div>
    </main>
  )
}
