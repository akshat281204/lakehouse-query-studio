import type { HistogramBucket, TimelineBucket } from '../api/types'
import { compact } from '../lib/format'

/** Stacked bars of query activity over time (succeeded / failed / cancelled). */
export function ActivityChart({
  timeline,
  windowSeconds,
}: {
  timeline: TimelineBucket[]
  windowSeconds: number
}) {
  const W = 760
  const H = 230
  const pad = { l: 38, r: 8, t: 10, b: 28 }
  const totals = timeline.map((b) => b.succeeded + b.failed + b.cancelled)
  const max = Math.max(1, ...totals)
  const niceMax = max <= 4 ? max : Math.ceil(max / 4) * 4
  const bw = (W - pad.l - pad.r) / Math.max(timeline.length, 1)
  const y = (v: number) => pad.t + (H - pad.t - pad.b) * (1 - v / niceMax)
  const fmt = (iso: string) => {
    const d = new Date(iso)
    return windowSeconds > 86400
      ? d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
      : d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })
  }
  const empty = totals.every((t) => t === 0)
  const ticks = [0, niceMax / 2, niceMax]
  const labelIdx = [0, Math.floor(timeline.length / 2), timeline.length - 1]

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      role="img"
      aria-label="Query activity over time"
      className="chart-svg"
    >
      {ticks.map((t) => (
        <g key={t}>
          <line
            x1={pad.l}
            x2={W - pad.r}
            y1={y(t)}
            y2={y(t)}
            className="grid-line"
          />
          <text x={pad.l - 6} y={y(t) + 3.5} textAnchor="end" className="axis">
            {compact(t)}
          </text>
        </g>
      ))}
      {timeline.map((b, i) => {
        const x = pad.l + i * bw + bw * 0.14
        const w = bw * 0.72
        let top = H - pad.b
        const seg = (n: number, cls: string) => {
          if (n === 0) return null
          const h = (H - pad.t - pad.b) * (n / niceMax)
          top -= h
          return (
            <rect
              key={cls}
              x={x}
              y={top}
              width={w}
              height={Math.max(h, 1)}
              rx={2}
              className={cls}
            />
          )
        }
        return (
          <g key={b.start} data-testid="activity-bar">
            {seg(b.succeeded, 'bar-ok')}
            {seg(b.failed, 'bar-err')}
            {seg(b.cancelled, 'bar-warn')}
            <rect
              x={pad.l + i * bw}
              y={pad.t}
              width={bw}
              height={H - pad.t - pad.b}
              fill="transparent"
            >
              <title>{`${fmt(b.start)}: ${b.succeeded} succeeded, ${b.failed} failed, ${b.cancelled} cancelled`}</title>
            </rect>
          </g>
        )
      })}
      {labelIdx.map((i, k) =>
        timeline[i] ? (
          <text
            key={k}
            x={pad.l + i * bw + bw / 2}
            y={H - 8}
            textAnchor={k === 0 ? 'start' : k === 2 ? 'end' : 'middle'}
            className="axis"
          >
            {fmt(timeline[i].start)}
          </text>
        ) : null,
      )}
      {empty ? (
        <text x={W / 2} y={H / 2} textAnchor="middle" className="axis-empty">
          No queries in this window
        </text>
      ) : null}
    </svg>
  )
}

export interface DonutSegment {
  label: string
  value: number
  className: string
}

/** Ring chart of query outcomes with the total in the middle. */
export function Donut({ segments }: { segments: DonutSegment[] }) {
  const total = segments.reduce((a, s) => a + s.value, 0)
  const R = 54
  const C = 2 * Math.PI * R
  let offset = 0
  return (
    <svg
      viewBox="0 0 140 140"
      role="img"
      aria-label="Query outcomes"
      className="donut"
    >
      <circle cx="70" cy="70" r={R} className="donut-track" />
      {total > 0
        ? segments
            .filter((s) => s.value > 0)
            .map((s) => {
              const len = (s.value / total) * C
              const el = (
                <circle
                  key={s.label}
                  cx="70"
                  cy="70"
                  r={R}
                  className={`donut-seg ${s.className}`}
                  strokeDasharray={`${len} ${C - len}`}
                  strokeDashoffset={-offset}
                  transform="rotate(-90 70 70)"
                >
                  <title>{`${s.label}: ${s.value}`}</title>
                </circle>
              )
              offset += len
              return el
            })
        : null}
      <text x="70" y="68" textAnchor="middle" className="donut-total">
        {compact(total)}
      </text>
      <text x="70" y="86" textAnchor="middle" className="axis">
        queries
      </text>
    </svg>
  )
}

/** Distribution of succeeded-query latency. */
export function LatencyHistogram({ buckets }: { buckets: HistogramBucket[] }) {
  const W = 520
  const H = 170
  const pad = { l: 8, r: 8, t: 22, b: 34 }
  const max = Math.max(1, ...buckets.map((b) => b.count))
  const bw = (W - pad.l - pad.r) / buckets.length
  const total = buckets.reduce((a, b) => a + b.count, 0)
  const bound = (v: number) => (v >= 1000 ? `${v / 1000}s` : `${v}ms`)
  const label = (b: HistogramBucket, i: number) =>
    b.leMs === 0 ? `>${bound(buckets[i - 1]?.leMs ?? 0)}` : `≤${bound(b.leMs)}`
  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      role="img"
      aria-label="Latency distribution"
      className="chart-svg"
    >
      {buckets.map((b, i) => {
        const h = (H - pad.t - pad.b) * (b.count / max)
        const x = pad.l + i * bw
        return (
          <g key={i} data-testid="latency-bar">
            <rect
              x={x + bw * 0.12}
              y={H - pad.b - h}
              width={bw * 0.76}
              height={Math.max(b.count ? h : 0, b.count ? 1 : 0)}
              rx={2}
              className="bar-accent"
            >
              <title>{`${label(b, i)}: ${b.count} queries`}</title>
            </rect>
            {b.count > 0 ? (
              <text
                x={x + bw / 2}
                y={H - pad.b - h - 4}
                textAnchor="middle"
                className="axis"
              >
                {b.count}
              </text>
            ) : null}
            {i % 2 === 0 || buckets.length < 9 ? (
              <text
                x={x + bw / 2}
                y={H - 14}
                textAnchor="middle"
                className="axis"
              >
                {label(b, i).replace(' ', '')}
              </text>
            ) : null}
          </g>
        )
      })}
      {total === 0 ? (
        <text x={W / 2} y={H / 2} textAnchor="middle" className="axis-empty">
          No completed queries yet
        </text>
      ) : null}
    </svg>
  )
}
