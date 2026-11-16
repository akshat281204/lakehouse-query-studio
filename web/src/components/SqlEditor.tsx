import { useRef } from 'react'
import { tokenize } from '../lib/sql'

interface SqlEditorProps {
  value: string
  onChange: (v: string) => void
  onRun: () => void
  disabled?: boolean
}

/** A textarea with a highlighted mirror behind it. Highlighting is cosmetic. */
export function SqlEditor({
  value,
  onChange,
  onRun,
  disabled,
}: SqlEditorProps) {
  const mirror = useRef<HTMLPreElement>(null)
  return (
    <div className="sql-editor">
      <pre ref={mirror} className="sql-mirror" aria-hidden="true">
        {tokenize(value).map((t, i) => (
          <span key={i} className={`tok-${t.kind}`}>
            {t.text}
          </span>
        ))}
        {'\n'}
      </pre>
      <textarea
        aria-label="SQL statement"
        className="sql-input"
        spellCheck={false}
        value={value}
        disabled={disabled}
        onChange={(e) => onChange(e.target.value)}
        onScroll={(e) => {
          if (mirror.current) {
            mirror.current.scrollTop = e.currentTarget.scrollTop
            mirror.current.scrollLeft = e.currentTarget.scrollLeft
          }
        }}
        onKeyDown={(e) => {
          if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
            e.preventDefault()
            onRun()
          }
        }}
      />
    </div>
  )
}
