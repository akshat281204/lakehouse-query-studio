import { describe, it, expect, vi, beforeEach } from 'vitest'
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
      getDatasetProfile: vi.fn(),
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

beforeEach(() => {
  vi.resetAllMocks()
  mockedApi.getDatasetProfile.mockResolvedValue({
    rows: 0,
    files: 0,
    columns: [],
  })
})

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

  it('shows min, max and null counts from the profile, and a dash where there is no range', async () => {
    mockedApi.getDataset.mockResolvedValue({
      ...detail,
      schema: [
        ...detail.schema,
        {
          position: 1,
          name: 'blob',
          type: 'binary',
          nullable: true,
          isPartition: false,
        },
      ],
    })
    mockedApi.getDatasetPreview.mockResolvedValue({
      columns: detail.schema,
      rows: [{ id: 1 }],
      truncated: false,
    })
    mockedApi.getDatasetProfile.mockResolvedValue({
      rows: 2000,
      files: 3,
      note: '1 column(s) have no range: their type is not summarised',
      columns: [
        {
          name: 'id',
          type: 'int64',
          nullable: false,
          isPartition: false,
          hasRange: true,
          min: '-3',
          max: '9007199254740993',
          nullCount: 0,
        },
        {
          name: 'blob',
          type: 'binary',
          nullable: true,
          isPartition: false,
          hasRange: false,
        },
      ],
    })
    render(<DatasetDetailPage id="d1" onBack={() => {}} />)
    expect(await screen.findByTestId('profile-summary')).toHaveTextContent(
      '2,000 rows in 3 files',
    )
    expect(screen.getByText('-3')).toBeInTheDocument()
    // a 64-bit extreme is shown exactly, not rounded through a JS number
    expect(screen.getByText('9007199254740993')).toBeInTheDocument()
    expect(screen.getAllByText('—').length).toBeGreaterThanOrEqual(3) // blob min, max, nulls
    expect(screen.getByText(/have no range/)).toBeInTheDocument()
  })

  it('still shows the schema when the profile cannot be loaded', async () => {
    mockedApi.getDataset.mockResolvedValue(detail)
    mockedApi.getDatasetPreview.mockResolvedValue({
      columns: detail.schema,
      rows: [{ id: 1 }],
      truncated: false,
    })
    mockedApi.getDatasetProfile.mockRejectedValue(
      new ApiError(503, 'unavailable', 'object store down'),
    )
    render(<DatasetDetailPage id="d1" onBack={() => {}} />)
    expect(
      await screen.findByText(
        /column statistics unavailable: object store down/i,
      ),
    ).toBeInTheDocument()
    expect(screen.getAllByText('int64').length).toBeGreaterThan(0)
    expect(screen.queryByText('Min')).toBeNull()
  })
})
