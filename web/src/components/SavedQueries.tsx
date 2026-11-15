import { useState } from 'react'
import type { SavedQuery } from '../api/types'
import { timeAgo } from '../lib/format'

interface Props {
  items: SavedQuery[] | null
  error: string | null
  activeId?: string
  onLoad: (q: SavedQuery) => void
  onDelete: (q: SavedQuery) => Promise<void>
}

/** The saved-query library: click a name to load it into the editor. Deleting
 * asks for a second click so a stray click cannot remove a query. */
export function SavedQueries({
  items,
  error,
  activeId,
  onLoad,
  onDelete,
}: Props) {
  const [confirming, setConfirming] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const [deleteError, setDeleteError] = useState<string | null>(null)

  const remove = async (q: SavedQuery) => {
    setBusy(true)
    setDeleteError(null)
    try {
      await onDelete(q)
      setConfirming(null)
    } catch (e) {
      setDeleteError(e instanceof Error ? e.message : String(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <aside className="saved-queries" aria-label="Saved queries">
      <h2>Saved queries</h2>
      {error ? (
        <p className="error" role="alert">
          {error}
        </p>
      ) : null}
      {!error && items === null ? <p className="muted">Loading…</p> : null}
      {items && items.length === 0 ? (
        <p className="muted">
          Nothing saved yet. Write a query and choose Save.
        </p>
      ) : null}
      {deleteError ? (
        <p className="error" role="alert">
          {deleteError}
        </p>
      ) : null}
      <ul>
        {items?.map((q) => (
          <li key={q.id} className={q.id === activeId ? 'active' : undefined}>
            <button
              type="button"
              className="saved-name"
              title={q.description || q.statement}
              aria-current={q.id === activeId ? 'true' : undefined}
              onClick={() => onLoad(q)}
            >
              {q.name}
            </button>
            <span className="muted saved-when">{timeAgo(q.updatedAt)}</span>
            {confirming === q.id ? (
              <span className="saved-confirm">
                <button
                  type="button"
                  className="btn-small btn-danger"
                  disabled={busy}
                  onClick={() => void remove(q)}
                >
                  Delete
                </button>
                <button
                  type="button"
                  className="btn-small btn-ghost"
                  disabled={busy}
                  onClick={() => setConfirming(null)}
                >
                  Keep
                </button>
              </span>
            ) : (
              <button
                type="button"
                className="saved-remove"
                aria-label={`Delete ${q.name}`}
                title="Delete"
                onClick={() => setConfirming(q.id)}
              >
                ×
              </button>
            )}
          </li>
        ))}
      </ul>
    </aside>
  )
}
