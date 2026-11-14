export function formatDuration(ms: number | undefined): string {
  if (ms === undefined) return '—'
  if (ms < 1000) return `${ms} ms`
  return `${(ms / 1000).toFixed(ms < 10_000 ? 2 : 1)} s`
}

export function formatBytes(n: number | undefined): string {
  if (n === undefined) return '—'
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`
  return `${(n / 1024 / 1024).toFixed(1)} MiB`
}

export const TERMINAL = ['SUCCEEDED', 'FAILED', 'CANCELLED', 'TIMED_OUT']

export function isTerminal(status: string): boolean {
  return TERMINAL.includes(status)
}

/** "just now", "5m ago", "3h ago", "2d ago". */
export function timeAgo(iso: string | undefined, now = Date.now()): string {
  if (!iso) return '—'
  const s = Math.max(0, Math.round((now - new Date(iso).getTime()) / 1000))
  if (s < 10) return 'just now'
  if (s < 60) return `${s}s ago`
  if (s < 3600) return `${Math.floor(s / 60)}m ago`
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`
  return `${Math.floor(s / 86400)}d ago`
}

/** 1234 -> "1.2K", 1_500_000 -> "1.5M". */
export function compact(n: number): string {
  const abs = Math.abs(n)
  if (abs >= 1e9) return `${(n / 1e9).toFixed(1)}B`
  if (abs >= 1e6) return `${(n / 1e6).toFixed(1)}M`
  if (abs >= 1e4) return `${(n / 1e3).toFixed(0)}K`
  if (abs >= 1e3) return `${(n / 1e3).toFixed(1)}K`
  return String(Math.round(n))
}

/** Milliseconds as "12 ms", "1.4 s", "2m 5s". */
export function ms(v: number): string {
  if (!v) return '0 ms'
  if (v < 1000) return `${Math.round(v)} ms`
  if (v < 60_000) return `${(v / 1000).toFixed(v < 10_000 ? 2 : 1)} s`
  return `${Math.floor(v / 60_000)}m ${Math.round((v % 60_000) / 1000)}s`
}

export function formatBytesLong(n: number): string {
  if (n < 1024) return `${n} B`
  const units = ['KiB', 'MiB', 'GiB', 'TiB']
  let v = n / 1024
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${v.toFixed(v < 10 ? 2 : 1)} ${units[i]}`
}
