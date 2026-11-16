import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { ResultsPanel } from './ResultsPanel'
import { api } from '../api/client'
import type { QueryJob, QueryResults } from '../api/types'

vi.mock('../api/client', async (orig) => {
  const real = await orig<typeof import('../api/client')>()
  return {
    ...real,
    api: {
      getQueryResults: vi.fn(),
      queryExportUrl: (id: string) => `/api/v1/queries/${id}/export`,
    },
  }
})
const m = vi.mocked(api)

const job: QueryJob = {
  id: 'q1',
  statement: 'SELECT 1',
  statementHash: 'h',
  datasetVersionIds: [],
  status: 'SUCCEEDED',
  truncated: false,
  attempt: 1,
  cancelRequested: false,
  createdAt: '2024-01-01T00:00:00Z',
  durationMs: 46,
  bytesStaged: 2048,
  resultBytes: 512,
}

const results = (rows: QueryResults['rows']): QueryResults => ({
  columns: [
    { name: 'region', type: 'string' },
    { name: 'price', type: 'double' },
  ],
  rows,
  totalRows: rows.length,
  offset: 0,
  returned: rows.length,
  nextOffset: null,
  truncated: false,
})

describe('ResultsPanel', () => {
  beforeEach(() => vi.clearAllMocks())

  it('gives every value in a decimal column the same number of decimals', async () => {
    m.getQueryResults.mockResolvedValue(
      results([
        ['eu', 200.93],
        ['us', 57.15],
        ['ap', 1902.9],
      ]),
    )
    render(<ResultsPanel job={job} />)
    expect(await screen.findByText('1,902.90')).toBeInTheDocument()
    expect(screen.getByText('200.93')).toBeInTheDocument()
    expect(screen.getByText('57.15')).toBeInTheDocument()
  })

  it('shows no decimals when a double column holds whole numbers', async () => {
    m.getQueryResults.mockResolvedValue(
      results([
        ['eu', 3],
        ['us', 12],
      ]),
    )
    render(<ResultsPanel job={job} />)
    expect(await screen.findByText('12')).toBeInTheDocument()
    expect(screen.queryByText('12.00')).toBeNull()
  })

  it('shows run details, column types and row numbers', async () => {
    m.getQueryResults.mockResolvedValue(results([['eu', 1.5]]))
    render(<ResultsPanel job={job} />)
    const meta = await screen.findByRole('list', { name: /run details/i })
    expect(within(meta).getByText('46 ms')).toBeInTheDocument()
    expect(within(meta).getByText('2.0 KiB')).toBeInTheDocument()
    expect(screen.getByText('double')).toBeInTheDocument()
    expect(
      screen.getByRole('columnheader', { name: /row number/i }),
    ).toBeInTheDocument()
  })

  it('keeps null values visible and never renders markup', async () => {
    m.getQueryResults.mockResolvedValue(
      results([['<b>x</b>', null as unknown as number]]),
    )
    render(<ResultsPanel job={job} />)
    expect(await screen.findByText('<b>x</b>')).toBeInTheDocument()
    expect(screen.getByText('null')).toBeInTheDocument()
  })
})
