import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { DatasetDetailPage } from './DatasetDetailPage'
import { api, ApiError } from '../api/client'

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    api: {
      getDataset: vi.fn(),
      getDatasetPreview: vi.fn(),
    },
  }
})

const mockedApi = vi.mocked(api)

const detail = {
  id: 'd1',
  namespace: 'default',
  name: 'sales',
  sourcePrefix: 'data/sales',
  format: 'parquet',
  status: 'ACTIVE',
  retainOriginal: false,
  createdAt: '2024-01-01T00:00:00Z',
  updatedAt: '2024-01-01T00:00:00Z',
  version: {
    id: 'v1',
    datasetId: 'd1',
    version: 1,
    objectPrefix: 'data/sales',
    format: 'parquet',
    partitionColumns: [],
    status: 'READY',
    rowCount: 2,
    createdAt: '2024-01-01T00:00:00Z',
  },
  schema: [
    {
      position: 0,
      name: 'id',
      type: 'int64',
      nullable: false,
      isPartition: false,
    },
  ],
}

describe('DatasetDetailPage', () => {
  it('shows an error state when the detail request fails', async () => {
    mockedApi.getDataset.mockRejectedValue(
      new ApiError(404, 'not_found', 'dataset missing'),
    )
    mockedApi.getDatasetPreview.mockResolvedValue({
      columns: [],
      rows: [],
      truncated: false,
    })

    render(<DatasetDetailPage id="missing" onBack={() => {}} />)

    const alert = await screen.findByRole('alert')
    expect(alert).toHaveTextContent('dataset missing')
  })

  it('renders schema and preview rows', async () => {
    mockedApi.getDataset.mockResolvedValue(detail)
    mockedApi.getDatasetPreview.mockResolvedValue({
      columns: detail.schema,
      rows: [{ id: 1 }],
      totalRows: 2,
      truncated: false,
    })

    render(<DatasetDetailPage id="d1" onBack={() => {}} />)

    expect(await screen.findByText('sales')).toBeInTheDocument()
    expect(screen.getAllByText('int64').length).toBeGreaterThan(0)
    // The column name appears in both the schema table and the preview header.
    expect(screen.getAllByText('id').length).toBeGreaterThan(0)
  })
})
