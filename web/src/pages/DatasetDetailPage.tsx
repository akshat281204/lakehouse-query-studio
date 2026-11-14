import { useEffect, useState } from 'react'
import { api, ApiError } from '../api/client'
import type { DatasetDetail, PreviewResponse } from '../api/types'

interface DatasetDetailPageProps {
  id: string
  onBack: () => void
}

export function DatasetDetailPage({ id, onBack }: DatasetDetailPageProps) {
  const [detail, setDetail] = useState<DatasetDetail | null>(null)
  const [preview, setPreview] = useState<PreviewResponse | null>(null)
  const [previewError, setPreviewError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    setDetail(null)
    setPreview(null)
    setPreviewError(null)

    api
      .getDataset(id)
      .then((d) => setDetail(d))
      .catch((err: unknown) => {
        setError(err instanceof ApiError ? err.message : String(err))
      })
      .finally(() => setLoading(false))

    api
      .getDatasetPreview(id)
      .then((p) => setPreview(p))
      .catch((err: unknown) => {
        setPreviewError(err instanceof ApiError ? err.message : String(err))
      })
  }, [id])

  return (
    <main className="page">
      <header className="page-header">
        <button type="button" className="back-button" onClick={onBack}>
          ← Datasets
        </button>
        <h1>{detail ? detail.name : 'Dataset'}</h1>
      </header>

      {loading ? <p>Loading dataset…</p> : null}
      {error ? (
        <p className="error" role="alert">
          {error}
        </p>
      ) : null}

      {detail ? (
        <>
          <section className="dataset-info">
            <dl>
              <dt>Source prefix</dt>
              <dd>{detail.sourcePrefix}</dd>
              <dt>Format</dt>
              <dd>{detail.format}</dd>
              <dt>Status</dt>
              <dd>{detail.status}</dd>
              <dt>Version</dt>
              <dd>
                v{detail.version.version} ({detail.version.status})
              </dd>
              {detail.version.rowCount !== undefined ? (
                <>
                  <dt>Rows</dt>
                  <dd>{detail.version.rowCount.toLocaleString()}</dd>
                </>
              ) : null}
            </dl>
          </section>

          <section>
            <h2>Schema</h2>
            <table className="data-table">
              <thead>
                <tr>
                  <th>Column</th>
                  <th>Type</th>
                  <th>Nullable</th>
                  <th>Partition</th>
                </tr>
              </thead>
              <tbody>
                {detail.schema.map((c) => (
                  <tr key={`${c.position}-${c.name}`}>
                    <td>{c.name}</td>
                    <td>{c.type}</td>
                    <td>{c.nullable ? 'yes' : 'no'}</td>
                    <td>{c.isPartition ? 'yes' : 'no'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          <section>
            <h2>Preview</h2>
            {previewError ? <p className="error">{previewError}</p> : null}
            {!previewError && !preview ? <p>Loading preview…</p> : null}
            {preview && preview.note ? (
              <p className="note">{preview.note}</p>
            ) : null}
            {preview && preview.rows.length > 0 ? (
              <>
                <table className="data-table">
                  <thead>
                    <tr>
                      {preview.columns.map((c) => (
                        <th key={`${c.position}-${c.name}`}>{c.name}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {preview.rows.map((row, i) => (
                      <tr key={i}>
                        {preview.columns.map((c) => (
                          <td key={`${c.position}-${c.name}`}>
                            {String(row[c.name] ?? '')}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
                {preview.truncated ? (
                  <p className="note">Showing a sample of rows.</p>
                ) : null}
              </>
            ) : null}
          </section>
        </>
      ) : null}
    </main>
  )
}
