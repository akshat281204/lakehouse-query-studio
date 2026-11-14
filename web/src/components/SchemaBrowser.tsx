import { useEffect, useState } from 'react'
import { api, ApiError } from '../api/client'
import type { DatasetListItem, SchemaColumn } from '../api/types'
import { relationRef } from '../lib/sql'

interface SchemaBrowserProps {
  onInsert: (text: string) => void
}

export function SchemaBrowser({ onInsert }: SchemaBrowserProps) {
  const [datasets, setDatasets] = useState<DatasetListItem[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [open, setOpen] = useState<
    Record<string, SchemaColumn[] | 'loading' | string>
  >({})

  useEffect(() => {
    api
      .listDatasets({ limit: 100 })
      .then((r) =>
        setDatasets(r.datasets.filter((d) => d.versionStatus === 'READY')),
      )
      .catch((e: unknown) =>
        setError(e instanceof ApiError ? e.message : String(e)),
      )
  }, [])

  const toggle = async (d: DatasetListItem) => {
    if (open[d.id]) {
      setOpen((o) => {
        const rest = { ...o }
        delete rest[d.id]
        return rest
      })
      return
    }
    setOpen((o) => ({ ...o, [d.id]: 'loading' }))
    try {
      const s = await api.getDatasetSchema(d.id)
      setOpen((o) => ({ ...o, [d.id]: s.columns }))
    } catch (e) {
      setOpen((o) => ({
        ...o,
        [d.id]: e instanceof ApiError ? e.message : String(e),
      }))
    }
  }

  return (
    <aside className="schema-browser" aria-label="Schema browser">
      <h2>Datasets</h2>
      {error ? (
        <p className="error" role="alert">
          {error}
        </p>
      ) : null}
      {!error && datasets === null ? <p>Loading datasets…</p> : null}
      {datasets && datasets.length === 0 ? (
        <p>No queryable datasets yet.</p>
      ) : null}
      <ul>
        {datasets?.map((d) => {
          const state = open[d.id]
          return (
            <li key={d.id}>
              <button
                type="button"
                className="link-button"
                aria-expanded={!!state}
                onClick={() => void toggle(d)}
              >
                {state ? '▾' : '▸'} {d.name}
              </button>{' '}
              <button
                type="button"
                className="link-button"
                title="Insert name"
                onClick={() => onInsert(relationRef(d.name))}
              >
                + insert
              </button>
              {state === 'loading' ? <p>Loading schema…</p> : null}
              {typeof state === 'string' && state !== 'loading' ? (
                <p className="error">{state}</p>
              ) : null}
              {Array.isArray(state) ? (
                <ul className="schema-columns">
                  {state.map((c) => (
                    <li key={c.position}>
                      <button
                        type="button"
                        className="link-button"
                        onClick={() => onInsert(c.name)}
                      >
                        {c.name}
                      </button>{' '}
                      <span className="muted">{c.type}</span>
                    </li>
                  ))}
                </ul>
              ) : null}
            </li>
          )
        })}
      </ul>
    </aside>
  )
}
