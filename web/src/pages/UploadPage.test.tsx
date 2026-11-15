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
      listDatasets: vi.fn(),
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
    mockedApi.listDatasets.mockResolvedValue({
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
    // The job stays RUNNING until the test has observed that state; a fixed
    // RUNNING-then-SUCCEEDED sequence races the observer on a loaded machine.
    let finished = false
    mockedApi.getIngestion.mockImplementation(async () =>
      job({ status: finished ? 'SUCCEEDED' : 'RUNNING', attempt: 1 }),
    )
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
    finished = true
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

  describe('Parquet and append', () => {
    const parquetInspect: InspectResponse = {
      format: 'parquet',
      rowCount: 2000,
      sample: [],
      note: 'a sample cannot be shown for this file, but it can still be imported',
      columns: [
        { name: 'id', type: 'int64', nullable: false },
        { name: 'region', type: 'string', nullable: false },
      ],
    }
    const pickParquet = async () => {
      const file = new File(['PAR1'], 'orders.parquet')
      fireEvent.change(screen.getByLabelText('Data file'), {
        target: { files: [file] },
      })
      await screen.findByText('Inferred schema')
      return file
    }

    it('imports a Parquet file as it is: no partition choice, its own button', async () => {
      mockedApi.inspectUpload.mockResolvedValue(parquetInspect)
      mockedApi.uploadDataset.mockResolvedValue(
        job({ inputFormat: 'parquet', status: 'QUEUED' }),
      )
      render(<UploadPage onOpenDataset={() => {}} pollMs={5} />)
      await pickParquet()
      expect(screen.getByText(/imported exactly as it is/i)).toBeInTheDocument()
      expect(screen.queryByLabelText(/Partition by/)).toBeNull()
      expect(screen.queryByText('Partition by')).toBeNull()
      expect(screen.getByLabelText('Dataset name')).toHaveValue('orders')
      fireEvent.click(screen.getByText('Import Parquet'))
      await waitFor(() =>
        expect(mockedApi.uploadDataset).toHaveBeenCalledWith(
          expect.any(File),
          expect.objectContaining({
            name: 'orders',
            partitionColumns: undefined,
          }),
        ),
      )
    })

    it('checks a file against the chosen dataset and offers the append only when compatible', async () => {
      mockedApi.inspectUpload.mockResolvedValue({
        ...inspected,
        append: {
          dataset: 'sales',
          compatible: true,
          issues: [],
          datasetRows: 1000,
          latestVersion: 1,
        },
      })
      mockedApi.uploadDataset.mockResolvedValue(
        job({ mode: 'append', version: 2, status: 'QUEUED' }),
      )
      render(<UploadPage onOpenDataset={() => {}} pollMs={5} />)
      await pickFile()
      fireEvent.click(screen.getByLabelText('Add to an existing dataset'))
      expect(screen.getByText(/choose the dataset to add/i)).toBeInTheDocument()
      fireEvent.change(await screen.findByLabelText('Existing dataset'), {
        target: { value: 'sales' },
      })
      await waitFor(() =>
        expect(mockedApi.inspectUpload).toHaveBeenLastCalledWith(
          expect.any(File),
          'sales',
        ),
      )
      expect(await screen.findByRole('status')).toHaveTextContent(
        /compatible.*adds.*rows to sales.*currently 1,000 rows/i,
      )
      // an append has no name field, partition choice or keep-original option
      expect(screen.queryByLabelText('Dataset name')).toBeNull()
      expect(screen.queryByLabelText(/Partition by/)).toBeNull()
      fireEvent.click(screen.getByText('Add to sales'))
      await waitFor(() =>
        expect(mockedApi.uploadDataset).toHaveBeenCalledWith(
          expect.any(File),
          expect.objectContaining({ name: 'sales', mode: 'append' }),
        ),
      )
      expect(await screen.findByText(/Appending to/)).toBeInTheDocument()
    })

    it('lists why a file cannot be added and blocks the upload', async () => {
      mockedApi.inspectUpload.mockResolvedValue({
        ...inspected,
        append: {
          dataset: 'sales',
          compatible: false,
          issues: [
            'column "id" is string in the new data but int64 in the dataset',
            'column "x" is not in the dataset; adding columns is not supported yet',
          ],
        },
      })
      render(<UploadPage onOpenDataset={() => {}} pollMs={5} />)
      await pickFile()
      fireEvent.click(screen.getByLabelText('Add to an existing dataset'))
      fireEvent.change(await screen.findByLabelText('Existing dataset'), {
        target: { value: 'sales' },
      })
      const alert = await screen.findByRole('alert')
      expect(alert).toHaveTextContent('cannot be added to sales')
      expect(alert).toHaveTextContent('column "id" is string')
      expect(alert).toHaveTextContent('adding columns is not supported yet')
      expect(screen.getByText('Add to sales')).toBeDisabled()
      expect(mockedApi.uploadDataset).not.toHaveBeenCalled()
    })

    it('says plainly that reusing a name replaces the data', async () => {
      mockedApi.inspectUpload.mockResolvedValue(inspected)
      render(<UploadPage onOpenDataset={() => {}} pollMs={5} />)
      await pickFile()
      expect(
        screen.getByText(/replaces its data with this file as a new version/i),
      ).toBeInTheDocument()
    })
  })
})
