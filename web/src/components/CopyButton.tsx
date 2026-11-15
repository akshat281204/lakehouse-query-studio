import { useState } from 'react'

/** Copies text to the clipboard and confirms inline; stays quiet if copying is blocked. */
export function CopyButton({
  text,
  label = 'Copy',
}: {
  text: string
  label?: string
}) {
  const [done, setDone] = useState(false)
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(text)
      setDone(true)
      setTimeout(() => setDone(false), 1400)
    } catch {
      /* clipboard unavailable: nothing to confirm */
    }
  }
  return (
    <button
      type="button"
      className="btn-small btn-ghost"
      onClick={() => void copy()}
      aria-label={`${label} statement`}
    >
      {done ? 'Copied' : label}
    </button>
  )
}
