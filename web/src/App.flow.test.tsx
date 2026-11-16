import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import App from './App'

/**
 * Drives the real App, client and pages end to end against a small fake API
 * implemented at the fetch boundary: pick a dataset, run SQL, see results,
 * chart them, and get the export link. (jsdom, not a real browser.)
 */
function json(body: unknown, status = 200) {
  return Promise.resolve(
    new Response(JSON.stringify(body), {
      status,
      headers: { 'Content-Type': 'application/json' },
    }),
  )
}

const jobs: Record<string, { statement: string; polls: number }> = {}
let submitted: string[] = []
let n = 0

function fakeApi(
  input: RequestInfo | URL,
  init?: RequestInit,
): Promise<Response> {
  const url = new URL(String(input), 'http://localhost')
  const path = url.pathname
  const method = init?.method ?? 'GET'
  const base = {
    datasetVersionIds: [],
    truncated: false,
    attempt: 1,
    cancelRequested: false,
    createdAt: '2024-01-01T00:00:00Z',
    statementHash: 'h',
  }
  if (path === '/readyz')
    return json({
      status: 'ok',
      dependencies: { postgres: { ok: true }, minio: { ok: true } },
    })
  if (path === '/api/v1/datasets' && method === 'GET') {
    return json({
      datasets: [
        {
          id: 'd1',
          namespace: 'default',
          name: 'sales',
          sourcePrefix: 'p',
          format: 'parquet',
          status: 'ACTIVE',
          version: 1,
          versionStatus: 'READY',
          createdAt: '',
        },
      ],
      offset: 0,
      limit: 100,
      total: 1,
    })
  }
  if (path === '/api/v1/datasets/d1/schema') {
    return json({
      columns: [
        {
          position: 0,
          name: 'region',
          type: 'string',
          nullable: false,
          isPartition: false,
        },
        {
          position: 1,
          name: 'amt',
          type: 'double',
          nullable: false,
          isPartition: false,
        },
      ],
    })
  }
  if (path === '/api/v1/ingestions') return json({ jobs: [] })
  if (path === '/api/v1/queries' && method === 'POST') {
    const { statement } = JSON.parse(String(init?.body)) as {
      statement: string
    }
    if (/drop/i.test(statement))
      return json(
        {
          error: {
            code: 'query_denied',
            message: 'only a single read-only SELECT statement is allowed',
          },
        },
        400,
      )
    submitted.push(statement)
    const id = `q${++n}`
    jobs[id] = { statement, polls: 0 }
    return json({ ...base, id, statement, status: 'QUEUED' }, 202)
  }
  if (path === '/api/v1/queries' && method === 'GET') {
    return json({
      queries: Object.entries(jobs).map(([id, j]) => ({
        ...base,
        id,
        statement: j.statement,
        status: 'SUCCEEDED',
        durationMs: 12,
        rowsReturned: 2,
      })),
      offset: 0,
      limit: 50,
    })
  }
  const m = path.match(/^\/api\/v1\/queries\/(q\d+)(\/results)?$/)
  if (m && m[2]) {
    return json({
      columns: [
        { name: 'region', type: 'string' },
        { name: 'total', type: 'double' },
      ],
      rows: [
        ['eu', 10],
        ['us', 30],
      ],
      offset: 0,
      returned: 2,
      nextOffset: null,
      totalRows: 2,
      truncated: false,
    })
  }
  if (m) {
    const j = jobs[m[1]]
    j.polls++
    return json({
      ...base,
      id: m[1],
      statement: j.statement,
      status: j.polls < 2 ? 'RUNNING' : 'SUCCEEDED',
      rowsReturned: 2,
      durationMs: 12,
    })
  }
  return json({ error: { code: 'not_found', message: 'not found' } }, 404)
}

beforeEach(() => {
  submitted = []
  n = 0
  for (const k of Object.keys(jobs)) delete jobs[k]
  window.location.hash = '#/query'
  vi.stubGlobal('fetch', vi.fn(fakeApi))
})
afterEach(() => vi.unstubAllGlobals())

describe('workbench flow', () => {
  it('select dataset → run SQL → results → chart → export → history → rerun', async () => {
    render(<App />)
    fireEvent.change(screen.getByLabelText('SQL statement'), {
      target: { value: 'SELECT region, sum(amt) AS total FROM' },
    })
    fireEvent.click(await screen.findByTitle('Insert name'))
    fireEvent.click(screen.getByRole('button', { name: 'Run' }))

    expect(
      await screen.findByTestId('result-summary', undefined, { timeout: 5000 }),
    ).toHaveTextContent('2 rows × 2 columns')
    expect(submitted).toEqual(['SELECT region, sum(amt) AS total FROM sales'])
    expect(screen.getByText('eu')).toBeInTheDocument()

    fireEvent.click(screen.getByRole('tab', { name: 'Chart' }))
    expect(screen.getAllByTestId('chart-mark')).toHaveLength(2)

    expect(screen.getByRole('link', { name: /export csv/i })).toHaveAttribute(
      'href',
      '/api/v1/queries/q1/export',
    )

    // History lists it and Rerun submits a fresh job with the same text.
    window.location.hash = '#/history'
    fireEvent.click(await screen.findByRole('button', { name: 'Rerun' }))
    await waitFor(() => expect(submitted).toHaveLength(2), { timeout: 5000 })
    expect(submitted[1]).toBe(submitted[0])
    expect(
      await screen.findByTestId('result-summary', undefined, { timeout: 5000 }),
    ).toBeInTheDocument()
  })

  it('shows the server denial for unsafe SQL', async () => {
    render(<App />)
    fireEvent.change(screen.getByLabelText('SQL statement'), {
      target: { value: 'DROP TABLE sales' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Run' }))
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'read-only SELECT',
    )
    expect(submitted).toHaveLength(0)
  })

  it('renders the overview as the landing page', async () => {
    window.location.hash = '#/'
    render(<App />)
    expect(await screen.findByTestId('stat-datasets')).toHaveTextContent('1')
  })
})
