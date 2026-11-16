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
})
