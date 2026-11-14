import { describe, it, expect, vi, afterEach } from 'vitest'
import { api, ApiError } from './client'

describe('api client', () => {
  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('returns parsed JSON on success', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: true,
        status: 200,
        statusText: 'OK',
        json: async () => ({ status: 'ok' }),
      }),
    )

    const res = await api.health()
    expect(res.status).toBe('ok')
  })

  it('requests the backend-reachable service-info route', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({ service: 'lakehouse-api', version: 'dev' }),
    })
    vi.stubGlobal('fetch', fetchMock)

    const res = await api.serviceInfo()

    expect(res).toEqual({ service: 'lakehouse-api', version: 'dev' })
    expect(fetchMock).toHaveBeenCalledWith('/api/v1/info', undefined)
  })

  it('throws an ApiError with the API error code on failure', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue({
        ok: false,
        status: 503,
        statusText: 'Service Unavailable',
        json: async () => ({
          error: { code: 'unavailable', message: 'postgres down' },
        }),
      }),
    )

    await expect(api.ready()).rejects.toMatchObject({
      name: 'ApiError',
      status: 503,
      code: 'unavailable',
    })
  })

  it('throws an ApiError when the network request fails', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockRejectedValue(new Error('connection refused')),
    )

    await expect(api.serviceInfo()).rejects.toBeInstanceOf(ApiError)
  })

  it('lists datasets with query parameters', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({ datasets: [], offset: 0, limit: 10, total: 0 }),
    })
    vi.stubGlobal('fetch', fetchMock)

    await api.listDatasets({ q: 'sales', limit: 10 })

    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/datasets?limit=10&q=sales',
      undefined,
    )
  })

  it('registers a dataset with a JSON POST body', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 201,
      statusText: 'Created',
      json: async () => ({ id: 'd1', name: 'sales' }),
    })
    vi.stubGlobal('fetch', fetchMock)

    await api.registerDataset({ name: 'sales', prefix: 'data/sales' })

    expect(fetchMock).toHaveBeenCalledWith('/api/v1/datasets', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: 'sales', prefix: 'data/sales' }),
    })
  })

  it('posts the raw file with upload metadata in the query string', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 202,
      statusText: 'Accepted',
      json: async () => ({ id: 'j1' }),
    })
    vi.stubGlobal('fetch', fetchMock)
    const file = new File(['a\n1\n'], 'my data.csv')

    await api.uploadDataset(file, {
      name: 'sales',
      partitionColumns: ['region', 'year'],
      retainOriginal: true,
    })

    const [url, init] = fetchMock.mock.calls[0] as [string, RequestInit]
    const parsed = new URL(url, 'http://x')
    expect(parsed.pathname).toBe('/api/v1/ingestions')
    expect(parsed.searchParams.get('name')).toBe('sales')
    expect(parsed.searchParams.get('filename')).toBe('my data.csv')
    expect(parsed.searchParams.get('partitionColumns')).toBe('region,year')
    expect(parsed.searchParams.get('retainOriginal')).toBe('true')
    expect(init.method).toBe('POST')
    expect(init.body).toBe(file)
  })

  it('polls and retries ingestion jobs by encoded id', async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({ id: 'a/b' }),
    })
    vi.stubGlobal('fetch', fetchMock)

    await api.getIngestion('a/b')
    await api.retryIngestion('a/b')

    expect(fetchMock.mock.calls[0][0]).toBe('/api/v1/ingestions/a%2Fb')
    expect(fetchMock.mock.calls[1][0]).toBe('/api/v1/ingestions/a%2Fb/retry')
    expect((fetchMock.mock.calls[1][1] as RequestInit).method).toBe('POST')
  })
})
