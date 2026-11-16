import { useEffect, useMemo, useRef, useState } from 'react'
import { api } from '../api/client'
import { THEMES } from '../lib/theme'

interface Command {
  id: string
  group: 'Go to' | 'Saved queries' | 'Datasets' | 'Theme'
  label: string
  hint?: string
  run: () => void
}

interface Props {
  open: boolean
  onClose: () => void
  onNavigate: (hash: string) => void
  onPreview: (datasetName: string) => void
  onOpenSaved: (statement: string) => void
  onTheme: (id: string) => void
}

const PAGES: { label: string; hash: string; hint: string }[] = [
  { label: 'Overview', hash: '#/', hint: 'health and activity' },
  { label: 'Datasets', hash: '#/datasets', hint: 'browse and register' },
  { label: 'SQL workbench', hash: '#/query', hint: 'run a query' },
  { label: 'Query history', hash: '#/history', hint: 'rerun past queries' },
  { label: 'Upload data', hash: '#/upload', hint: 'CSV, JSON or JSONL' },
]

/** Cmd/Ctrl+K launcher: jump to a page or dataset, preview rows, switch theme. */
export function CommandPalette({
  open,
  onClose,
  onNavigate,
  onPreview,
  onOpenSaved,
  onTheme,
}: Props) {
  const [q, setQ] = useState('')
  const [active, setActive] = useState(0)
  const [datasets, setDatasets] = useState<{ id: string; name: string }[]>([])
  const [saved, setSaved] = useState<
    { id: string; name: string; statement: string; description: string }[]
  >([])
  const input = useRef<HTMLInputElement>(null)
  const list = useRef<HTMLUListElement>(null)

  useEffect(() => {
    if (!open) return
    setQ('')
    setActive(0)
    input.current?.focus()
    let alive = true
    api
      .listDatasets({ limit: 100 })
      .then((r) => alive && setDatasets(r.datasets))
      .catch(() => alive && setDatasets([]))
    api
      .listSavedQueries()
      .then((r) => alive && setSaved(r.savedQueries))
      .catch(() => alive && setSaved([]))
    return () => {
      alive = false
    }
  }, [open])

  const commands = useMemo<Command[]>(() => {
    const done = (fn: () => void) => () => {
      onClose()
      fn()
    }
    return [
      ...PAGES.map<Command>((p) => ({
        id: `p-${p.hash}`,
        group: 'Go to',
        label: p.label,
        hint: p.hint,
        run: done(() => onNavigate(p.hash)),
      })),
      ...saved.map<Command>((q) => ({
        id: `s-${q.id}`,
        group: 'Saved queries',
        label: q.name,
        hint: q.description || 'open in the editor',
        run: done(() => onOpenSaved(q.statement)),
      })),
      ...datasets.flatMap<Command>((d) => [
        {
          id: `d-${d.id}`,
          group: 'Datasets',
          label: d.name,
          hint: 'open dataset',
          run: done(() => onNavigate(`#/datasets/${d.id}`)),
        },
        {
          id: `q-${d.id}`,
          group: 'Datasets',
          label: `Preview rows from ${d.name}`,
          hint: 'run a query',
          run: done(() => onPreview(d.name)),
        },
      ]),
      ...THEMES.map<Command>((t) => ({
        id: `t-${t.id}`,
        group: 'Theme',
        label: `${t.label} theme`,
        hint: t.dark ? 'dark' : 'light',
        run: done(() => onTheme(t.id)),
      })),
    ]
  }, [datasets, saved, onClose, onNavigate, onPreview, onOpenSaved, onTheme])

  const shown = useMemo(() => {
    const needle = q.trim().toLowerCase()
    return needle
      ? commands.filter((c) =>
          `${c.label} ${c.hint ?? ''}`.toLowerCase().includes(needle),
        )
      : commands.filter((c) => c.group !== 'Datasets' || c.id.startsWith('d-'))
  }, [commands, q])

  useEffect(() => setActive(0), [q])
  useEffect(() => {
    list.current
      ?.querySelector<HTMLElement>('[aria-selected="true"]')
      ?.scrollIntoView?.({ block: 'nearest' })
  }, [active, shown])

  if (!open) return null

  const onKey = (e: React.KeyboardEvent) => {
    if (e.key === 'Escape') {
      e.preventDefault()
      onClose()
    } else if (e.key === 'ArrowDown') {
      e.preventDefault()
      setActive((a) => Math.min(shown.length - 1, a + 1))
    } else if (e.key === 'ArrowUp') {
      e.preventDefault()
      setActive((a) => Math.max(0, a - 1))
    } else if (e.key === 'Enter') {
      e.preventDefault()
      shown[active]?.run()
    }
  }

  let lastGroup = ''
  return (
    <div className="palette-backdrop" onMouseDown={onClose}>
      <div
        className="palette"
        role="dialog"
        aria-modal="true"
        aria-label="Command palette"
        onMouseDown={(e) => e.stopPropagation()}
        onKeyDown={onKey}
      >
        <input
          ref={input}
          className="palette-input"
          placeholder="Search pages, datasets and themes…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
          aria-label="Search commands"
          role="combobox"
          aria-expanded="true"
          aria-controls="palette-list"
          autoComplete="off"
          spellCheck={false}
        />
        <ul
          className="palette-list"
          id="palette-list"
          role="listbox"
          ref={list}
        >
          {shown.length === 0 ? (
            <li className="palette-empty">Nothing matches “{q}”.</li>
          ) : null}
          {shown.map((c, i) => {
            const head = c.group !== lastGroup
            lastGroup = c.group
            return (
              <li key={c.id} role="none">
                {head ? <div className="palette-group">{c.group}</div> : null}
                <div
                  role="option"
                  aria-selected={i === active}
                  className="palette-item"
                  onMouseMove={() => setActive(i)}
                  onClick={c.run}
                >
                  <span>{c.label}</span>
                  {c.hint ? <small>{c.hint}</small> : null}
                </div>
              </li>
            )
          })}
        </ul>
        <div className="palette-foot">
          <span>
            <kbd>↑</kbd>
            <kbd>↓</kbd> move
          </span>
          <span>
            <kbd>↵</kbd> open
          </span>
          <span>
            <kbd>esc</kbd> close
          </span>
        </div>
      </div>
    </div>
  )
}
