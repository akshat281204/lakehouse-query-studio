import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react'
import { UploadPage } from './UploadPage'
import { api, ApiError } from '../api/client'
import type { IngestionJob, InspectResponse } from '../api/types'

vi.mock('../api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('../api/client')>()
  return {
    ...actual,
    api: {
      inspectUpload: vi.fn(),
      uploadDataset: vi.fn(),
      getIngestion: vi.fn(),
      retryIngestion: vi.fn(),
    },
  }
})

const mockedApi = vi.mocked(api)

const inspected: InspectResponse = {
  format: 'csv',
  rowCount: 2,
  columns: [
    { name: 'id', type: 'int64', nullable: false },
    { name: 'region', type: 'string', nullable: true },
    { name: 'amount', type: 'double', nullable: true },
  ],
  sample: [
    { id: 1, region: 'us', amount: 1.5 },
    { id: 2, region: null, amount: null },
  ],
}

function job(over: Partial<IngestionJob>): IngestionJob {
  return {
    id: 'j1',
    datasetId: 'd1',
    datasetName: 'sales',
    versionId: 'v1',
    version: 1,
    kind: 'convert',
    status: 'QUEUED',
    inputFormat: 'csv',
    partitionColumns: [],
    retryable: false,
    attempt: 0,
    createdAt: '2024-01-01T00:00:00Z',
    ...over,
  }
}

async function pickFile() {
  const input = screen.getByLabelText('Data file')
  const file = new File(['id,region\n1,us\n'], 'Sales Data.csv', {
    type: 'text/csv',
  })
  fireEvent.change(input, { target: { files: [file] } })
  await screen.findByText('Inferred schema')
  return file
}

describe('UploadPage', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  afterEach(() => {
    cleanup()
  })

  it('shows the inferred schema, sample, and a suggested name', async () => {
    mockedApi.inspectUpload.mockResolvedValue(inspected)
    render(<UploadPage onOpenDataset={() => {}} pollMs={5} />)
    await pickFile()

    expect(screen.getByText('int64')).toBeInTheDocument()
    expect(screen.getByLabelText('Dataset name')).toHaveValue('sales-data')
    // Floating-point columns cannot be partition keys.
    expect(screen.getByLabelText('Partition by amount')).toBeDisabled()
    expect(screen.getByLabelText('Partition by region')).toBeEnabled()
    expect(screen.getAllByText('∅').length).toBe(2)
  })

  it('shows an actionable error when inspection fails', async () => {
    mockedApi.inspectUpload.mockRejectedValue(
      new ApiError(422, 'invalid_input', 'row at line 3 has 1 fields'),
    )
    render(<UploadPage onOpenDataset={() => {}} pollMs={5} />)
    fireEvent.change(screen.getByLabelText('Data file'), {
      target: { files: [new File(['x'], 'bad.csv')] },
    })
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'row at line 3 has 1 fields',
    )
    expect(screen.queryByText('Upload and convert')).not.toBeInTheDocument()
  })

  it('uploads with partitions and follows the job to success', async () => {
    mockedApi.inspectUpload.mockResolvedValue(inspected)
    mockedApi.uploadDataset.mockResolvedValue(job({ status: 'QUEUED' }))
    mockedApi.getIngestion
      .mockResolvedValueOnce(job({ status: 'RUNNING', attempt: 1 }))
      .mockResolvedValue(job({ status: 'SUCCEEDED', attempt: 1 }))
    const onOpen = vi.fn()
    render(<UploadPage onOpenDataset={onOpen} pollMs={5} />)
    const file = await pickFile()

    fireEvent.click(screen.getByLabelText('Partition by region'))
    fireEvent.click(screen.getByText('Upload and convert'))

    await waitFor(() =>
      expect(mockedApi.uploadDataset).toHaveBeenCalledWith(file, {
        name: 'sales-data',
        partitionColumns: ['region'],
        retainOriginal: false,
      }),
    )
    expect(await screen.findByText('Converting to Parquet…')).toBeVisible()
    const open = await screen.findByText('Open dataset')
    fireEvent.click(open)
    expect(onOpen).toHaveBeenCalledWith('d1')
  })

  it('shows a non-retryable failure without a retry button', async () => {
    mockedApi.inspectUpload.mockResolvedValue(inspected)
    mockedApi.uploadDataset.mockResolvedValue(
      job({
        status: 'FAILED',
        errorSummary: 'file has a header but no data rows',
        retryable: false,
      }),
    )
    render(<UploadPage onOpenDataset={() => {}} pollMs={5} />)
    await pickFile()
    fireEvent.click(screen.getByText('Upload and convert'))

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'file has a header but no data rows',
    )
    expect(screen.queryByText('Retry')).not.toBeInTheDocument()
  })

  it('offers retry for a retryable failure and follows the new job', async () => {
    mockedApi.inspectUpload.mockResolvedValue(inspected)
    mockedApi.uploadDataset.mockResolvedValue(
      job({
        status: 'FAILED',
        errorSummary: 'the object store was unavailable; retry the job',
        retryable: true,
      }),
    )
    mockedApi.retryIngestion.mockResolvedValue(
      job({ id: 'j2', version: 2, status: 'SUCCEEDED', retryOf: 'j1' }),
    )
    render(<UploadPage onOpenDataset={() => {}} pollMs={5} />)
    await pickFile()
    fireEvent.click(screen.getByText('Upload and convert'))

    fireEvent.click(await screen.findByText('Retry'))
    await waitFor(() =>
      expect(mockedApi.retryIngestion).toHaveBeenCalledWith('j1'),
    )
    expect(await screen.findByText('Open dataset')).toBeInTheDocument()
    expect(screen.getByText(/version 2/)).toBeInTheDocument()
  })

  it('surfaces upload errors from the API', async () => {
    mockedApi.inspectUpload.mockResolvedValue(inspected)
    mockedApi.uploadDataset.mockRejectedValue(
      new ApiError(400, 'invalid_argument', 'dataset name must be lowercase'),
    )
    render(<UploadPage onOpenDataset={() => {}} pollMs={5} />)
    await pickFile()
    fireEvent.click(screen.getByText('Upload and convert'))
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'dataset name must be lowercase',
    )
  })

  it('keeps the last known job state when a poll fails', async () => {
    mockedApi.inspectUpload.mockResolvedValue(inspected)
    mockedApi.uploadDataset.mockResolvedValue(
      job({ status: 'RUNNING', attempt: 1 }),
    )
    mockedApi.getIngestion.mockRejectedValue(
      new ApiError(0, 'network', 'request failed'),
    )
    render(<UploadPage onOpenDataset={() => {}} pollMs={5} />)
    await pickFile()
    fireEvent.click(screen.getByText('Upload and convert'))

    expect(await screen.findByRole('status')).toHaveTextContent(
      'request failed',
    )
    expect(screen.getByText('RUNNING')).toBeInTheDocument()
  })
})
