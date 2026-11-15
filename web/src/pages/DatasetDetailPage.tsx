import { useEffect, useState } from 'react'
import { api, ApiError } from '../api/client'
import type {
  DatasetDetail,
  DatasetProfile,
  PreviewResponse,
  ProfileColumn,
} from '../api/types'

interface DatasetDetailPageProps {
  id: string
  onBack: () => void
}

const byName = (p: DatasetProfile | null) =>
  new Map<string, ProfileColumn>((p?.columns ?? []).map((c) => [c.name, c]))

export function DatasetDetailPage({ id, onBack }: DatasetDetailPageProps) {
  const [detail, setDetail] = useState<DatasetDetail | null>(null)
  const [preview, setPreview] = useState<PreviewResponse | null>(null)
  const [previewError, setPreviewError] = useState<string | null>(null)
  const [profile, setProfile] = useState<DatasetProfile | null>(null)
  const [profileError, setProfileError] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    setDetail(null)
    setPreview(null)
    setPreviewError(null)
    setProfile(null)
    setProfileError(null)

    api
      .getDatasetProfile(id)
      .then((p) => setProfile(p))
      .catch((err: unknown) => {
        setProfileError(err instanceof ApiError ? err.message : String(err))
      })

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
              {detail.version.errorSummary ? (
                <>
                  <dt>Error</dt>
                  <dd className="error">{detail.version.errorSummary}</dd>
                </>
              ) : null}
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
            {profile && profile.files > 0 && profile.rows > 0 ? (
              <p className="profile-summary" data-testid="profile-summary">
                <strong>{profile.rows.toLocaleString()}</strong> rows in{' '}
                <strong>{profile.files.toLocaleString()}</strong>{' '}
                {profile.files === 1 ? 'file' : 'files'}
                <span className="muted">
                  {' '}
                  · from file footers, no data scanned
                </span>
              </p>
            ) : null}
            <div className="table-scroll">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Type</th>
                    <th>Nullable</th>
                    <th>Partition</th>
                    {profile ? (
                      <>
                        <th>Min</th>
                        <th>Max</th>
                        <th className="num">Nulls</th>
                      </>
                    ) : null}
                  </tr>
                </thead>
                <tbody>
                  {detail.schema.map((c) => {
                    const p = byName(profile).get(c.name)
                    return (
                      <tr key={`${c.position}-${c.name}`}>
                        <td>{c.name}</td>
                        <td>{c.type}</td>
                        <td>{c.nullable ? 'yes' : 'no'}</td>
                        <td>{c.isPartition ? 'yes' : 'no'}</td>
                        {profile ? (
                          <>
                            <td title={p?.min}>
                              {p?.hasRange ? (
                                <>
                                  {p.min}
                                  {p.truncated ? '…' : ''}
                                </>
                              ) : (
                                <span className="muted">—</span>
                              )}
                            </td>
                            <td title={p?.max}>
                              {p?.hasRange ? (
                                <>
                                  {p.max}
                                  {p.truncated ? '…' : ''}
                                </>
                              ) : (
                                <span className="muted">—</span>
                              )}
                            </td>
                            <td className="num">
                              {p?.nullCount === undefined ? (
                                <span className="muted">—</span>
                              ) : (
                                p.nullCount.toLocaleString()
                              )}
                            </td>
                          </>
                        ) : null}
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
            {profile?.note ? <p className="note">{profile.note}</p> : null}
            {profileError ? (
              <p className="note">
                Column statistics unavailable: {profileError}
              </p>
            ) : null}
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
                <div className="table-scroll">
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
                            <td
                              key={`${c.position}-${c.name}`}
                              title={String(row[c.name] ?? '')}
                            >
                              {String(row[c.name] ?? '')}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
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
