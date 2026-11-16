import { describe, it, expect, vi, beforeEach } from 'vitest'
import {
  render,
  screen,
  fireEvent,
  waitFor,
  within,
} from '@testing-library/react'
import { OverviewPage } from './OverviewPage'
import { api, ApiError } from '../api/client'
import type { QueryJob, Summary } from '../api/types'

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    api: {
      ready: vi.fn(),
      getStats: vi.fn(),
      listQueries: vi.fn(),
      listIngestions: vi.fn(),
    },
  }
})
const m = vi.mocked(api)
beforeEach(() => vi.resetAllMocks())

const summary = (over: Partial<Summary> = {}): Summary => ({
  windowSeconds: 86400,
  generatedAt: '2025-01-01T00:00:00Z',
  queries: {
    total: 65,
    succeeded: 58,
    failed: 6,
    cancelled: 0,
    timedOut: 1,
    queued: 0,
    running: 0,
    rowsReturned: 25000,
    bytesStaged: 12 * 1024 * 1024,
    avgMs: 35,
    p50Ms: 30,
    p95Ms: 63,
    p99Ms: 120,
    maxMs: 130,
  },
  timeline: Array.from({ length: 24 }, (_, i) => ({
    start: new Date(Date.UTC(2025, 0, 1, i)).toISOString(),
    succeeded: i === 23 ? 58 : 0,
    failed: i === 23 ? 7 : 0,
    cancelled: 0,
    p50Ms: 30,
  })),
  latencyHistogram: [
    { leMs: 10, count: 1 },
    { leMs: 25, count: 24 },
    { leMs: 50, count: 27 },
    { leMs: 100, count: 6 },
    { leMs: 0, count: 0 },
  ],
  failureReasons: [
    { reason: 'engine_error', count: 6 },
    { reason: 'timeout', count: 1 },
  ],
  ingestions: { total: 3, succeeded: 2, failed: 1, active: 0 },
  datasets: 7,
  truncated: false,
  ...over,
})

const job = (
  status: QueryJob['status'],
  id: string,
  over: Partial<QueryJob> = {},
): QueryJob => ({
  id,
  statement: `SELECT ${id}`,
  statementHash: 'h',
  datasetVersionIds: [],
  status,
  truncated: false,
  attempt: 1,
  cancelRequested: false,
  createdAt: new Date().toISOString(),
  durationMs: 42,
  rowsReturned: 3,
  ...over,
})

function healthy() {
  m.ready.mockResolvedValue({
    status: 'ok',
    dependencies: { postgres: { ok: true }, objectstore: { ok: true } },
  })
  m.getStats.mockResolvedValue(summary())
  m.listQueries.mockResolvedValue({
    queries: [job('SUCCEEDED', '1'), job('FAILED', '2')],
    offset: 0,
    limit: 8,
  })
  m.listIngestions.mockResolvedValue({
    jobs: [
      {
        id: 'j1',
        datasetName: 'sales',
        version: 2,
        status: 'SUCCEEDED',
        createdAt: new Date().toISOString(),
      },
    ],
  } as never)
}

describe('OverviewPage', () => {
  it('shows KPIs computed by the server', async () => {
    healthy()
    render(<OverviewPage />)
    expect(await screen.findByTestId('kpi-queries')).toHaveTextContent('65')
    expect(screen.getByTestId('kpi-success-rate')).toHaveTextContent('89.2%')
    expect(screen.getByTestId('kpi-p50')).toHaveTextContent('30 ms')
    expect(screen.getByTestId('kpi-p95')).toHaveTextContent('63 ms')
    expect(screen.getByTestId('kpi-rows')).toHaveTextContent('25K')
    expect(screen.getByTestId('kpi-staged')).toHaveTextContent('12.0 MiB')
    expect(screen.getByTestId('kpi-datasets')).toHaveTextContent('7')
    expect(screen.getByTestId('kpi-queue')).toHaveTextContent('0')
    expect(m.getStats).toHaveBeenCalledWith('24h')
  })

  it('draws the activity timeline and latency histogram from the data', async () => {
    healthy()
    render(<OverviewPage />)
    await screen.findByTestId('kpi-queries')
    expect(screen.getAllByTestId('activity-bar')).toHaveLength(24)
    expect(screen.getAllByTestId('latency-bar')).toHaveLength(5)
    expect(
      screen.getByRole('img', { name: /query activity over time/i }),
    ).toBeInTheDocument()
    expect(
      screen.getByRole('img', { name: /latency distribution/i }),
    ).toBeInTheDocument()
    expect(
      screen.getByRole('img', { name: /query outcomes/i }),
    ).toBeInTheDocument()
  })

  it('lists failure reasons with friendly names and counts', async () => {
    healthy()
    render(<OverviewPage />)
    const panel = await screen.findByRole('region', { name: 'Failure reasons' })
    expect(within(panel).getByText(/engine error/i)).toBeInTheDocument()
    expect(within(panel).getByText('Timed out')).toBeInTheDocument()
    expect(within(panel).getByText('7 failed')).toBeInTheDocument()
  })

  it('says so when there are no failures', async () => {
    healthy()
    m.getStats.mockResolvedValue(
      summary({
        failureReasons: [],
        queries: { ...summary().queries, failed: 0, timedOut: 0 },
      }),
    )
    render(<OverviewPage />)
    expect(
      await screen.findByText(/no failures in this window/i),
    ).toBeInTheDocument()
  })

  it('switches the time window and reloads', async () => {
    healthy()
    render(<OverviewPage />)
    await screen.findByTestId('kpi-queries')
    fireEvent.click(screen.getByRole('button', { name: '1h' }))
    await waitFor(() => expect(m.getStats).toHaveBeenCalledWith('1h'))
    expect(screen.getByRole('button', { name: '1h' })).toHaveAttribute(
      'aria-pressed',
      'true',
    )
    fireEvent.click(screen.getByRole('button', { name: '7d' }))
    await waitFor(() => expect(m.getStats).toHaveBeenCalledWith('7d'))
  })

  it('shows health, recent queries and recent ingestions', async () => {
    healthy()
    render(<OverviewPage />)
    expect(await screen.findByText('PostgreSQL')).toBeInTheDocument()
    expect(screen.getByText('Object store')).toBeInTheDocument()
    expect(screen.getByText('SELECT 1')).toBeInTheDocument()
    expect(screen.getByText('FAILED')).toBeInTheDocument()
    expect(screen.getByText('sales')).toBeInTheDocument()
    expect(screen.getByTestId('ingest-summary')).toHaveTextContent('3 jobs')
  })

  it('shows a down dependency with its message', async () => {
    healthy()
    m.ready.mockResolvedValue({
      status: 'not_ready',
      dependencies: {
        postgres: { ok: false, message: 'connection refused' },
        objectstore: { ok: true },
      },
    })
    render(<OverviewPage />)
    expect(await screen.findByText('down')).toBeInTheDocument()
    expect(screen.getByText(/connection refused/)).toBeInTheDocument()
  })

  it('shows empty states for a fresh system', async () => {
    healthy()
    m.getStats.mockResolvedValue(
      summary({
        queries: {
          ...summary().queries,
          total: 0,
          succeeded: 0,
          failed: 0,
          timedOut: 0,
          rowsReturned: 0,
          bytesStaged: 0,
          p50Ms: 0,
          p95Ms: 0,
          maxMs: 0,
        },
        timeline: summary().timeline.map((b) => ({
          ...b,
          succeeded: 0,
          failed: 0,
        })),
        latencyHistogram: summary().latencyHistogram.map((b) => ({
          ...b,
          count: 0,
        })),
        failureReasons: [],
        datasets: 0,
      }),
    )
    m.listQueries.mockResolvedValue({ queries: [], offset: 0, limit: 8 })
    m.listIngestions.mockResolvedValue({ jobs: [] })
    render(<OverviewPage />)
    expect(await screen.findByText(/no queries yet/i)).toBeInTheDocument()
    expect(screen.getByText(/no uploads yet/i)).toBeInTheDocument()
    expect(screen.getByText(/no queries in this window/i)).toBeInTheDocument()
    expect(screen.getByText(/no completed queries yet/i)).toBeInTheDocument()
    // Nothing finished: no success rate or latency is invented.
    expect(screen.getByTestId('kpi-success-rate')).toHaveTextContent('—')
    expect(screen.getByTestId('kpi-p50')).toHaveTextContent('—')
  })

  it('degrades per section when requests fail and never invents numbers', async () => {
    m.ready.mockRejectedValue(new ApiError(0, 'network', 'offline'))
    m.getStats.mockRejectedValue(
      new ApiError(503, 'unavailable', 'queries are not configured'),
    )
    m.listQueries.mockRejectedValue(
      new ApiError(503, 'unavailable', 'catalog down'),
    )
    m.listIngestions.mockRejectedValue(
      new ApiError(503, 'unavailable', 'ingestion is not configured'),
    )
    render(<OverviewPage />)
    expect(await screen.findByText(/not ready: offline/i)).toBeInTheDocument()
    expect(screen.getAllByRole('alert').length).toBeGreaterThanOrEqual(3)
    expect(screen.getByTestId('kpi-queries')).toHaveTextContent('—')
    expect(screen.getByTestId('kpi-datasets')).toHaveTextContent('—')
  })

  it('shows a loading state first', () => {
    m.ready.mockReturnValue(new Promise(() => {}))
    m.getStats.mockReturnValue(new Promise(() => {}))
    m.listQueries.mockReturnValue(new Promise(() => {}))
    m.listIngestions.mockReturnValue(new Promise(() => {}))
    render(<OverviewPage />)
    expect(screen.getByText(/loading overview/i)).toBeInTheDocument()
  })
})
