import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import { OverviewPage } from './OverviewPage'
import { api, ApiError } from '../api/client'
import type { QueryJob } from '../api/types'

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    api: {
      ready: vi.fn(),
      listDatasets: vi.fn(),
      listQueries: vi.fn(),
      listIngestions: vi.fn(),
    },
  }
})
const m = vi.mocked(api)
beforeEach(() => vi.resetAllMocks())

const q = (status: QueryJob['status'], id: string): QueryJob => ({
  id,
  statement: `SELECT ${id}`,
  statementHash: 'h',
  datasetVersionIds: [],
  status,
  truncated: false,
  attempt: 1,
  cancelRequested: false,
  createdAt: '2024-01-01T00:00:00Z',
})

describe('OverviewPage', () => {
  it('shows counts derived from API data', async () => {
    m.ready.mockResolvedValue({
      status: 'ok',
      dependencies: { postgres: { ok: true }, minio: { ok: true } },
    })
    m.listDatasets.mockResolvedValue({
      datasets: [],
      offset: 0,
      limit: 1,
      total: 7,
    })
    m.listQueries.mockResolvedValue({
      queries: [
        q('RUNNING', '1'),
        q('QUEUED', '2'),
        q('FAILED', '3'),
        q('TIMED_OUT', '4'),
        q('SUCCEEDED', '5'),
      ],
      offset: 0,
      limit: 100,
    })
    m.listIngestions.mockResolvedValue({
      jobs: [
        { status: 'FAILED' },
        { status: 'RUNNING' },
        { status: 'SUCCEEDED' },
      ],
    } as never)
    render(<OverviewPage />)
    expect(await screen.findByTestId('stat-datasets')).toHaveTextContent('7')
    expect(screen.getByTestId('stat-active-queries')).toHaveTextContent('2')
    expect(screen.getByTestId('stat-failed-queries')).toHaveTextContent('2')
    expect(screen.getByTestId('stat-active-ingestions')).toHaveTextContent('1')
    expect(screen.getByTestId('stat-failed-ingestions')).toHaveTextContent('1')
    expect(screen.getByText('postgres:')).toBeInTheDocument()
    expect(screen.getByText('SELECT 5')).toBeInTheDocument()
  })

  it('shows a down dependency with its message', async () => {
    m.ready.mockResolvedValue({
      status: 'not_ready',
      dependencies: {
        postgres: { ok: false, message: 'connection refused' },
        minio: { ok: true },
      },
    })
    m.listDatasets.mockResolvedValue({
      datasets: [],
      offset: 0,
      limit: 1,
      total: 0,
    })
    m.listQueries.mockResolvedValue({ queries: [], offset: 0, limit: 100 })
    m.listIngestions.mockResolvedValue({ jobs: [] })
    render(<OverviewPage />)
    expect(await screen.findByText('down')).toBeInTheDocument()
    expect(screen.getByText(/connection refused/)).toBeInTheDocument()
    expect(screen.getByText(/no queries yet/i)).toBeInTheDocument()
  })

  it('degrades per section when requests fail and never invents numbers', async () => {
    m.ready.mockRejectedValue(new ApiError(0, 'network', 'offline'))
    m.listDatasets.mockRejectedValue(
      new ApiError(503, 'unavailable', 'catalog down'),
    )
    m.listQueries.mockRejectedValue(
      new ApiError(503, 'unavailable', 'queries are not configured'),
    )
    m.listIngestions.mockRejectedValue(
      new ApiError(503, 'unavailable', 'ingestion is not configured'),
    )
    render(<OverviewPage />)
    expect(await screen.findByText(/not ready: offline/i)).toBeInTheDocument()
    expect(screen.getAllByRole('alert').length).toBeGreaterThanOrEqual(3)
    expect(screen.getByTestId('stat-datasets')).toHaveTextContent('—')
    expect(screen.getByTestId('stat-failed-queries')).toHaveTextContent('—')
  })

  it('shows a loading state first', () => {
    m.ready.mockReturnValue(new Promise(() => {}))
    m.listDatasets.mockReturnValue(new Promise(() => {}))
    m.listQueries.mockReturnValue(new Promise(() => {}))
    m.listIngestions.mockReturnValue(new Promise(() => {}))
    render(<OverviewPage />)
    expect(screen.getByText(/loading overview/i)).toBeInTheDocument()
  })
})
