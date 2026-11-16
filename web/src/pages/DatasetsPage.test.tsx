import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { DatasetsPage } from './DatasetsPage'
import { api, ApiError } from '../api/client'

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    api: {
      listDatasets: vi.fn(),
      registerDataset: vi.fn(),
    },
  }
})

const mockedApi = vi.mocked(api)

describe('DatasetsPage', () => {
  it('shows the empty state when there are no datasets', async () => {
    mockedApi.listDatasets.mockResolvedValue({
      datasets: [],
      offset: 0,
      limit: 50,
      total: 0,
    })

    render(<DatasetsPage onOpen={() => {}} />)

    expect(await screen.findByText(/no datasets yet/i)).toBeInTheDocument()
  })

  it('shows an error state when the request fails', async () => {
    mockedApi.listDatasets.mockRejectedValue(
      new ApiError(503, 'unavailable', 'postgres down'),
    )

    render(<DatasetsPage onOpen={() => {}} />)

    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('postgres down')
  })

  it('renders the dataset list', async () => {
    mockedApi.listDatasets.mockResolvedValue({
      datasets: [
        {
          id: 'd1',
          namespace: 'default',
          name: 'sales',
          sourcePrefix: 'data/sales',
          format: 'parquet',
          status: 'ACTIVE',
          version: 1,
          versionStatus: 'READY',
          createdAt: '2024-01-01T00:00:00Z',
        },
      ],
      offset: 0,
      limit: 50,
      total: 1,
    })

    render(<DatasetsPage onOpen={() => {}} />)

    expect(await screen.findByText('sales')).toBeInTheDocument()
    expect(screen.getByText(/data\/sales/i)).toBeInTheDocument()
  })
})
