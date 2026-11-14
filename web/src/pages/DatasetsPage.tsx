import { useCallback, useEffect, useState } from 'react'
import type { FormEvent } from 'react'
import { api, ApiError } from '../api/client'
import type { DatasetListItem } from '../api/types'

interface DatasetsPageProps {
  onOpen: (id: string) => void
}

export function DatasetsPage({ onOpen }: DatasetsPageProps) {
  const [datasets, setDatasets] = useState<DatasetListItem[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [query, setQuery] = useState('')
  const [name, setName] = useState('')
  const [prefix, setPrefix] = useState('')
  const [submitting, setSubmitting] = useState(false)
  const [formError, setFormError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await api.listDatasets({ q: query || undefined })
      setDatasets(res.datasets)
      setTotal(res.total)
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [query])

  useEffect(() => {
    void load()
  }, [load])

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setSubmitting(true)
    setFormError(null)
    try {
      await api.registerDataset({ name, prefix })
      setName('')
      setPrefix('')
      await load()
    } catch (err) {
      setFormError(err instanceof ApiError ? err.message : String(err))
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <main className="page">
      <header className="page-header">
        <h1>Datasets</h1>
        <p>
          {total} dataset{total === 1 ? '' : 's'}
        </p>
      </header>

      <form className="register-form" onSubmit={handleSubmit}>
        <input
          aria-label="Dataset name"
          placeholder="name (e.g. sales)"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <input
          aria-label="Object prefix"
          placeholder="prefix (e.g. data/sales)"
          value={prefix}
          onChange={(e) => setPrefix(e.target.value)}
        />
        <button type="submit" disabled={submitting}>
          {submitting ? 'Registering…' : 'Register'}
        </button>
        {formError ? <p className="error">{formError}</p> : null}
      </form>

      <input
        aria-label="Search datasets"
        className="search-input"
        placeholder="Search datasets"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
      />

      {error ? (
        <p className="error" role="alert">
          {error}
        </p>
      ) : null}

      {loading && datasets.length === 0 ? <p>Loading datasets…</p> : null}

      {!loading && !error && datasets.length === 0 ? (
        <p className="empty">
          No datasets yet. Register a Parquet prefix above.
        </p>
      ) : null}

      {datasets.length > 0 ? (
        <ul className="dataset-list">
          {datasets.map((d) => (
            <li key={d.id}>
              <button
                type="button"
                className="dataset-item"
                onClick={() => onOpen(d.id)}
              >
                <span className="dataset-name">{d.name}</span>
                <span className="dataset-meta">
                  {d.sourcePrefix} · {d.format}
                  {d.version !== undefined ? ` · v${d.version}` : ''}
                </span>
              </button>
            </li>
          ))}
        </ul>
      ) : null}
    </main>
  )
}
