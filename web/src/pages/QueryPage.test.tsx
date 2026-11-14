import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor, fireEvent } from '@testing-library/react'
import { QueryPage } from './QueryPage'
import { api, ApiError } from '../api/client'
import type { QueryJob, QueryResults } from '../api/types'

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    api: {
      listDatasets: vi.fn(),
      getDatasetSchema: vi.fn(),
      submitQuery: vi.fn(),
      getQuery: vi.fn(),
      cancelQuery: vi.fn(),
      getQueryResults: vi.fn(),
      queryExportUrl: (id: string) => `/api/v1/queries/${id}/export`,
    },
  }
})
const m = vi.mocked(api)

const job = (over: Partial<QueryJob> = {}): QueryJob => ({
  id: 'q1',
  statement: 'SELECT 1',
  statementHash: 'h',
  datasetVersionIds: [],
  status: 'QUEUED',
  truncated: false,
  attempt: 1,
  cancelRequested: false,
  createdAt: '2024-01-01T00:00:00Z',
  ...over,
})
const results = (over: Partial<QueryResults> = {}): QueryResults => ({
  columns: [
    { name: 'region', type: 'string' },
    { name: 'total', type: 'double' },
  ],
  rows: [
    ['eu', 10],
    ['us', 20],
  ],
  offset: 0,
  returned: 2,
  nextOffset: null,
  totalRows: 2,
  truncated: false,
  ...over,
})

beforeEach(() => {
  vi.resetAllMocks()
  m.listDatasets.mockResolvedValue({
    datasets: [],
    offset: 0,
    limit: 100,
    total: 0,
  })
  m.queryExportUrl = ((id: string) =>
    `/api/v1/queries/${id}/export`) as typeof m.queryExportUrl
})

const run = () => fireEvent.click(screen.getByRole('button', { name: 'Run' }))

describe('QueryPage', () => {
  it('runs a query, polls to success, and shows counts, export link and details', async () => {
    m.submitQuery.mockResolvedValue(job())
    m.getQuery
      .mockResolvedValueOnce(job({ status: 'RUNNING' }))
      .mockResolvedValue(
        job({
          status: 'SUCCEEDED',
          rowsReturned: 2,
          durationMs: 42,
          bytesStaged: 2048,
          planSummary: 'SEQ_SCAN',
          attempt: 1,
        }),
      )
    m.getQueryResults.mockResolvedValue(results())
    render(<QueryPage pollMs={5} />)
    run()
    expect(await screen.findByTestId('result-summary')).toHaveTextContent(
      '2 rows × 2 columns',
    )
    expect(screen.getByText('eu')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /export csv/i })).toHaveAttribute(
      'href',
      '/api/v1/queries/q1/export',
    )
    expect(screen.getByTestId('query-status')).toHaveTextContent('SUCCEEDED')
    expect(m.submitQuery).toHaveBeenCalledWith('SELECT 1 AS one')

    fireEvent.click(screen.getByRole('tab', { name: 'Details' }))
    expect(screen.getByText('SEQ_SCAN')).toBeInTheDocument()
    expect(screen.getAllByText('42 ms').length).toBeGreaterThanOrEqual(2)
  })

  it('draws a chart from the loaded rows and lets the type change', async () => {
    m.submitQuery.mockResolvedValue(job({ status: 'SUCCEEDED' }))
    m.getQueryResults.mockResolvedValue(results())
    render(<QueryPage pollMs={5} />)
    run()
    await screen.findByTestId('result-summary')
    fireEvent.click(screen.getByRole('tab', { name: 'Chart' }))
    expect(screen.getAllByTestId('chart-mark')).toHaveLength(2)
    expect(
      screen.getByRole('img', { name: /bar chart of total by region/i }),
    ).toBeInTheDocument()
    fireEvent.change(screen.getByLabelText(/chart type/i), {
      target: { value: 'line' },
    })
    expect(screen.getByRole('img', { name: /line chart/i })).toBeInTheDocument()
  })

  it('explains when a result has nothing to chart', async () => {
    m.submitQuery.mockResolvedValue(job({ status: 'SUCCEEDED' }))
    m.getQueryResults.mockResolvedValue(
      results({ columns: [{ name: 's', type: 'string' }], rows: [['a']] }),
    )
    render(<QueryPage pollMs={5} />)
    run()
    await screen.findByTestId('result-summary')
    fireEvent.click(screen.getByRole('tab', { name: 'Chart' }))
    expect(screen.getByText(/at least one numeric column/i)).toBeInTheDocument()
  })

  it('pages through results', async () => {
    m.submitQuery.mockResolvedValue(job({ status: 'SUCCEEDED' }))
    m.getQueryResults
      .mockResolvedValueOnce(results({ nextOffset: 2, totalRows: 3 }))
      .mockResolvedValueOnce(
        results({ rows: [['ap', 5]], offset: 2, returned: 1, totalRows: 3 }),
      )
    render(<QueryPage pollMs={5} />)
    run()
    await screen.findByTestId('result-summary')
    expect(screen.getByRole('button', { name: 'Previous' })).toBeDisabled()
    fireEvent.click(screen.getByRole('button', { name: 'Next' }))
    expect(await screen.findByText('ap')).toBeInTheDocument()
    expect(m.getQueryResults).toHaveBeenLastCalledWith('q1', 2, 100)
    expect(screen.getByRole('button', { name: 'Next' })).toBeDisabled()
  })

  it('shows a safe message when the server denies the statement', async () => {
    m.submitQuery.mockRejectedValue(
      new ApiError(
        400,
        'query_denied',
        'only a single read-only SELECT statement is allowed',
      ),
    )
    render(<QueryPage pollMs={5} />)
    run()
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'only a single read-only SELECT',
    )
    expect(screen.getByRole('button', { name: 'Run' })).toBeEnabled()
  })

  it('shows failure reason and summary for a failed query', async () => {
    m.submitQuery.mockResolvedValue(
      job({
        status: 'FAILED',
        failureReason: 'engine_error',
        errorSummary: 'Binder Error: column not found',
      }),
    )
    render(<QueryPage pollMs={5} />)
    run()
    const a = await screen.findByRole('alert')
    expect(a).toHaveTextContent('engine_error')
    expect(a).toHaveTextContent('Binder Error: column not found')
    expect(m.getQueryResults).not.toHaveBeenCalled()
  })

  it('reports a timeout distinctly', async () => {
    m.submitQuery.mockResolvedValue(job({ status: 'TIMED_OUT' }))
    render(<QueryPage pollMs={5} />)
    run()
    expect(await screen.findByRole('alert')).toHaveTextContent(
      /execution timeout/i,
    )
  })

  it('shows a long-running state with elapsed time and cancels it', async () => {
    m.submitQuery.mockResolvedValue(job({ status: 'RUNNING' }))
    m.getQuery.mockResolvedValue(job({ status: 'RUNNING' }))
    m.cancelQuery.mockResolvedValue(
      job({ status: 'RUNNING', cancelRequested: true }),
    )
    render(<QueryPage pollMs={5} />)
    run()
    expect(await screen.findByText('Running…')).toBeInTheDocument()
    expect(screen.getByTestId('elapsed')).toBeInTheDocument()
    const cancel = screen.getByRole('button', { name: 'Cancel' })
    expect(cancel).toBeEnabled()
    // The worker notices the flag and the job turns CANCELLED on the next poll.
    m.getQuery.mockResolvedValue(job({ status: 'CANCELLED' }))
    fireEvent.click(cancel)
    expect(m.cancelQuery).toHaveBeenCalledWith('q1')
    expect(await screen.findByText(/was cancelled/i)).toBeInTheDocument()
    expect(screen.getByTestId('query-status')).toHaveTextContent('CANCELLED')
    expect(screen.getByRole('button', { name: 'Cancel' })).toBeDisabled()
  })

  it('shows an error when results cannot be loaded', async () => {
    m.submitQuery.mockResolvedValue(job({ status: 'SUCCEEDED' }))
    m.getQueryResults.mockRejectedValue(
      new ApiError(
        410,
        'result_expired',
        'the query result is no longer available',
      ),
    )
    render(<QueryPage pollMs={5} />)
    run()
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'no longer available',
    )
  })

  it('says so when a query returns no rows', async () => {
    m.submitQuery.mockResolvedValue(job({ status: 'SUCCEEDED' }))
    m.getQueryResults.mockResolvedValue(
      results({ rows: [], returned: 0, totalRows: 0 }),
    )
    render(<QueryPage pollMs={5} />)
    run()
    expect(await screen.findByText(/returned no rows/i)).toBeInTheDocument()
  })

  it('notes truncation', async () => {
    m.submitQuery.mockResolvedValue(job({ status: 'SUCCEEDED' }))
    m.getQueryResults.mockResolvedValue(results({ truncated: true }))
    render(<QueryPage pollMs={5} />)
    run()
    expect(await screen.findByTestId('result-summary')).toHaveTextContent(
      /truncated/i,
    )
  })

  it('renders cell values as text, never as markup', async () => {
    m.submitQuery.mockResolvedValue(job({ status: 'SUCCEEDED' }))
    m.getQueryResults.mockResolvedValue(
      results({
        columns: [{ name: 's', type: 'string' }],
        rows: [['<img src=x onerror=alert(1)>']],
        totalRows: 1,
        returned: 1,
      }),
    )
    const { container } = render(<QueryPage pollMs={5} />)
    run()
    await screen.findByTestId('result-summary')
    expect(container.querySelector('img')).toBeNull()
    expect(screen.getByText('<img src=x onerror=alert(1)>')).toBeInTheDocument()
  })

  it('auto-runs a handed-over statement once', async () => {
    m.submitQuery.mockResolvedValue(job({ status: 'SUCCEEDED' }))
    m.getQueryResults.mockResolvedValue(results())
    render(<QueryPage initialStatement="SELECT 7" autoRun pollMs={5} />)
    await screen.findByTestId('result-summary')
    expect(m.submitQuery).toHaveBeenCalledTimes(1)
    expect(m.submitQuery).toHaveBeenCalledWith('SELECT 7')
    expect(screen.getByLabelText('SQL statement')).toHaveValue('SELECT 7')
  })

  it('disables Run for an empty statement', () => {
    render(<QueryPage pollMs={5} />)
    fireEvent.change(screen.getByLabelText('SQL statement'), {
      target: { value: '   ' },
    })
    expect(screen.getByRole('button', { name: 'Run' })).toBeDisabled()
  })

  it('inserts dataset and column names from the schema browser', async () => {
    m.listDatasets.mockResolvedValue({
      datasets: [
        {
          id: 'd1',
          namespace: 'default',
          name: 'web-logs',
          sourcePrefix: 'p',
          format: 'parquet',
          status: 'ACTIVE',
          version: 1,
          versionStatus: 'READY',
          createdAt: '',
        },
        {
          id: 'd2',
          namespace: 'default',
          name: 'pending',
          sourcePrefix: 'p',
          format: 'parquet',
          status: 'ACTIVE',
          version: 1,
          versionStatus: 'PENDING',
          createdAt: '',
        },
      ],
      offset: 0,
      limit: 100,
      total: 2,
    })
    m.getDatasetSchema.mockResolvedValue({
      columns: [
        {
          position: 0,
          name: 'status',
          type: 'int64',
          nullable: false,
          isPartition: false,
        },
      ],
    })
    render(<QueryPage pollMs={5} />)
    fireEvent.change(screen.getByLabelText('SQL statement'), {
      target: { value: 'SELECT * FROM' },
    })
    expect(await screen.findByText(/web-logs/)).toBeInTheDocument()
    expect(screen.queryByText(/pending/)).toBeNull() // only READY versions are queryable
    fireEvent.click(screen.getByTitle('Insert name'))
    expect(screen.getByLabelText('SQL statement')).toHaveValue(
      'SELECT * FROM "web-logs"',
    )
    fireEvent.click(screen.getByRole('button', { name: /web-logs/ }))
    fireEvent.click(await screen.findByRole('button', { name: 'status' }))
    expect(screen.getByLabelText('SQL statement')).toHaveValue(
      'SELECT * FROM "web-logs" status',
    )
  })

  it('shows an error when the dataset list fails', async () => {
    m.listDatasets.mockRejectedValue(
      new ApiError(503, 'unavailable', 'catalog down'),
    )
    render(<QueryPage pollMs={5} />)
    await waitFor(() =>
      expect(screen.getByRole('alert')).toHaveTextContent('catalog down'),
    )
  })
})
