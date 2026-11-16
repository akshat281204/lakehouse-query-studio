import { useState } from 'react'
import type { SavedQuery } from '../api/types'

interface Props {
  /** The saved query currently open in the editor, if any. */
  active?: SavedQuery
  busy: boolean
  error: string | null
  onSave: (name: string, description: string, asNew: boolean) => void
  onCancel: () => void
}

/** Inline form under the run bar. With a saved query open it offers to update
 * it or to save a copy under a new name. */
export function SaveQueryForm({
  active,
  busy,
  error,
  onSave,
  onCancel,
}: Props) {
  const [name, setName] = useState(active?.name ?? '')
  const [description, setDescription] = useState(active?.description ?? '')
  const submit = (asNew: boolean) => {
    if (name.trim()) onSave(name, description, asNew)
  }
  return (
    <form
      className="save-form"
      aria-label="Save query"
      onSubmit={(e) => {
        e.preventDefault()
        submit(!active)
      }}
    >
      <label>
        Name
        <input
          type="text"
          value={name}
          maxLength={120}
          autoFocus
          onChange={(e) => setName(e.target.value)}
          placeholder="e.g. Revenue by region"
        />
      </label>
      <label>
        Description <span className="muted">(optional)</span>
        <input
          type="text"
          value={description}
          maxLength={500}
          onChange={(e) => setDescription(e.target.value)}
        />
      </label>
      <div className="save-actions">
        <button
          type="submit"
          className="btn-primary"
          disabled={busy || !name.trim()}
        >
          {active ? 'Update' : 'Save'}
        </button>
        {active ? (
          <button
            type="button"
            disabled={busy || !name.trim() || name.trim() === active.name}
            onClick={() => submit(true)}
            title={
              name.trim() === active.name
                ? 'Enter a different name to save a copy'
                : undefined
            }
          >
            Save as new
          </button>
        ) : null}
        <button type="button" className="btn-ghost" onClick={onCancel}>
          Cancel
        </button>
      </div>
      {error ? (
        <p className="error" role="alert">
          {error}
        </p>
      ) : null}
    </form>
  )
}
