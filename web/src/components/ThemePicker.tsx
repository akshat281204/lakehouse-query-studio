import { useEffect, useRef, useState } from 'react'
import { THEMES, applyTheme, loadTheme, saveTheme } from '../lib/theme'

function Swatch({ colors }: { colors: [string, string, string, string] }) {
  return (
    <span className="theme-swatch" aria-hidden="true">
      <i style={{ background: colors[0] }} />
      <i style={{ background: colors[2] }} />
      <i style={{ background: colors[1] }} />
      <i style={{ background: colors[3] }} />
    </span>
  )
}

export function ThemePicker() {
  const [current, setCurrent] = useState(loadTheme)
  const [open, setOpen] = useState(false)
  const root = useRef<HTMLDivElement>(null)

  useEffect(() => applyTheme(current), [current])

  useEffect(() => {
    if (!open) return
    const onDown = (e: MouseEvent) => {
      if (root.current && !root.current.contains(e.target as Node))
        setOpen(false)
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  const info = THEMES.find((t) => t.id === current) ?? THEMES[0]
  const choose = (id: string) => {
    setCurrent(id)
    saveTheme(id)
    setOpen(false)
  }

  return (
    <div className="theme-picker" ref={root}>
      <button
        type="button"
        className="theme-trigger"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-label={`Theme: ${info.label}`}
        onClick={() => setOpen((o) => !o)}
      >
        <Swatch colors={info.swatch} />
        {info.label}
      </button>
      {open ? (
        <ul className="theme-menu" role="menu" aria-label="Choose a theme">
          {THEMES.map((t) => (
            <li key={t.id} role="none">
              <button
                type="button"
                role="menuitemradio"
                aria-checked={t.id === current}
                onClick={() => choose(t.id)}
              >
                <Swatch colors={t.swatch} />
                {t.label}
                {t.id === current ? <span className="check">✓</span> : null}
              </button>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  )
}
