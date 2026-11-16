import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { HistoryPage } from './HistoryPage'
import { api, ApiError } from '../api/client'
import type { QueryJob } from '../api/types'

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return { ...actual, api: { listQueries: vi.fn() } }
})
const m = vi.mocked(api)
beforeEach(() => vi.resetAllMocks())

const job = (over: Partial<QueryJob>): QueryJob => ({
  id: 'q',
  statement: 'SELECT 1',
  statementHash: 'h',
  datasetVersionIds: [],
  status: 'SUCCEEDED',
  truncated: false,
  attempt: 1,
  cancelRequested: false,
  createdAt: '2024-01-01T00:00:00Z',
  ...over,
})

describe('HistoryPage', () => {
  it('shows loading, then an empty state', async () => {
    m.listQueries.mockResolvedValue({ queries: [], offset: 0, limit: 50 })
    render(<HistoryPage onRerun={() => {}} />)
    expect(screen.getByText(/loading history/i)).toBeInTheDocument()
    expect(await screen.findByText(/no queries yet/i)).toBeInTheDocument()
  })

  it('shows an error state', async () => {
    m.listQueries.mockRejectedValue(
      new ApiError(503, 'unavailable', 'queries are not configured'),
    )
    render(<HistoryPage onRerun={() => {}} />)
    expect(await screen.findByRole('alert')).toHaveTextContent('not configured')
  })

  it('lists status and duration and reruns the exact statement', async () => {
    m.listQueries.mockResolvedValue({
      queries: [
        job({
          id: 'a',
          statement: 'SELECT count(*) FROM sales',
          durationMs: 1500,
          rowsReturned: 1,
        }),
        job({ id: 'b', statement: 'SELECT nope', status: 'FAILED' }),
      ],
      offset: 0,
      limit: 50,
    })
    const onRerun = vi.fn()
    render(<HistoryPage onRerun={onRerun} />)
    expect(await screen.findByText('1.50 s')).toBeInTheDocument()
    expect(screen.getByText('FAILED')).toBeInTheDocument()
    fireEvent.click(screen.getAllByRole('button', { name: 'Rerun' })[0])
    expect(onRerun).toHaveBeenCalledWith('SELECT count(*) FROM sales')
  })
})
