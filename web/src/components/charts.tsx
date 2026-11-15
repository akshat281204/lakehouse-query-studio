import { useId, useState, type MouseEvent } from 'react'
import type { HistogramBucket, TimelineBucket } from '../api/types'
import { compact } from '../lib/format'

type Pt = [number, number]

/** Smooth path through points (Catmull-Rom converted to cubic Béziers). */
function smooth(pts: Pt[]): string {
  if (pts.length === 0) return ''
  if (pts.length === 1) return `M${pts[0][0]},${pts[0][1]}`
  let d = `M${pts[0][0]},${pts[0][1]}`
  for (let i = 0; i < pts.length - 1; i++) {
    const p0 = pts[i - 1] ?? pts[i]
    const p1 = pts[i]
    const p2 = pts[i + 1]
    const p3 = pts[i + 2] ?? p2
    const t = 0.18
    // Clamp control points between the two end values so the curve never
    // overshoots (a count dipping below zero, or above its neighbours).
    const lo = Math.min(p1[1], p2[1])
    const hi = Math.max(p1[1], p2[1])
    const cl = (v: number) => Math.min(hi, Math.max(lo, v))
    d += ` C${p1[0] + (p2[0] - p0[0]) * t},${cl(p1[1] + (p2[1] - p0[1]) * t)} ${p2[0] - (p3[0] - p1[0]) * t},${cl(p2[1] - (p3[1] - p1[1]) * t)} ${p2[0]},${p2[1]}`
  }
  return d
}

/** Small trend line with a soft fill, for a KPI. */
export function Sparkline({
  values,
  tone = 'accent',
  label,
}: {
  values: number[]
  tone?: 'accent' | 'ok' | 'info' | 'warn'
  label: string
}) {
  const id = useId()
  const W = 120
  const H = 36
  const pad = 3
  const max = Math.max(...values, 0)
  const min = Math.min(...values, 0)
  const span = max - min || 1
  const pts: Pt[] = values.map((v, i) => [
    pad + (i * (W - pad * 2)) / Math.max(values.length - 1, 1),
    H - pad - ((v - min) / span) * (H - pad * 2),
  ])
  const line = smooth(pts)
  const last = pts[pts.length - 1]
  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      className={`spark spark-${tone}`}
      role="img"
      aria-label={label}
      preserveAspectRatio="none"
    >
      <defs>
        <linearGradient id={id} x1="0" x2="0" y1="0" y2="1">
          <stop offset="0" className="spark-stop" stopOpacity="0.32" />
          <stop offset="1" className="spark-stop" stopOpacity="0" />
        </linearGradient>
      </defs>
      {pts.length > 1 ? (
        <>
          <path
            d={`${line} L${W - pad},${H} L${pad},${H} Z`}
            fill={`url(#${id})`}
          />
          <path d={line} className="spark-line" />
        </>
      ) : null}
      {last ? (
        <circle cx={last[0]} cy={last[1]} r="2.4" className="spark-dot" />
      ) : null}
    </svg>
  )
}

/** Query activity over time: stacked smooth areas with a hover read-out. */
export function ActivityChart({
  timeline,
  windowSeconds,
}: {
  timeline: TimelineBucket[]
  windowSeconds: number
}) {
  const uid = useId()
  const [hover, setHover] = useState<number | null>(null)
  const W = 760
  const H = 250
  const pad = { l: 34, r: 10, t: 14, b: 28 }
  const n = timeline.length
  const totals = timeline.map((b) => b.succeeded + b.failed + b.cancelled)
  const max = Math.max(1, ...totals)
  const niceMax = max <= 4 ? max : Math.ceil(max / 4) * 4
  const plotW = W - pad.l - pad.r
  const plotH = H - pad.t - pad.b
  const x = (i: number) => pad.l + (n <= 1 ? plotW / 2 : (i * plotW) / (n - 1))
  const y = (v: number) => pad.t + plotH * (1 - v / niceMax)
  const fmt = (iso: string) => {
    const d = new Date(iso)
    return windowSeconds > 86400
      ? d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
      : d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit' })
  }
  const empty = totals.every((t) => t === 0)
  const ticks = [0, niceMax / 2, niceMax]
  const labelIdx = [0, Math.floor((n - 1) / 2), n - 1]

  // cumulative boundaries: succeeded, +failed, +cancelled
  const c1 = timeline.map((b) => b.succeeded)
  const c2 = timeline.map((b) => b.succeeded + b.failed)
  const c3 = totals
  const top = (c: number[]): Pt[] => c.map((v, i) => [x(i), y(v)])
  const layer = (upper: number[], lower: number[] | null) => {
    const up = top(upper)
    const lo: Pt[] = lower
      ? top(lower).reverse()
      : [
          [x(n - 1), y(0)],
          [x(0), y(0)],
        ]
    if (up.length === 0) return ''
    const fwd = smooth(up)
    const back = smooth(lo).replace(/^M/, 'L')
    return `${fwd} ${back} Z`
  }

  const onMove = (e: MouseEvent<SVGSVGElement>) => {
    const r = e.currentTarget.getBoundingClientRect()
    const px = ((e.clientX - r.left) / r.width) * W
    const i = Math.round(((px - pad.l) / plotW) * (n - 1))
    setHover(Math.min(n - 1, Math.max(0, i)))
  }
  const hb = hover !== null ? timeline[hover] : null

  return (
    <div className="area-wrap">
      <svg
        viewBox={`0 0 ${W} ${H}`}
        role="img"
        aria-label="Query activity over time"
        className="chart-svg area-chart"
        onMouseMove={n > 1 ? onMove : undefined}
        onMouseLeave={() => setHover(null)}
      >
        <defs>
          {(['ok', 'err', 'warn'] as const).map((k) => (
            <linearGradient
              key={k}
              id={`${uid}-${k}`}
              x1="0"
              x2="0"
              y1="0"
              y2="1"
            >
              <stop offset="0" className={`grad-${k}`} stopOpacity="0.55" />
              <stop offset="1" className={`grad-${k}`} stopOpacity="0.04" />
            </linearGradient>
          ))}
        </defs>
        {ticks.map((t) => (
          <g key={t}>
            <line
              x1={pad.l}
              x2={W - pad.r}
              y1={y(t)}
              y2={y(t)}
              className="grid-line"
            />
            <text
              x={pad.l - 8}
              y={y(t) + 3.5}
              textAnchor="end"
              className="axis"
            >
              {compact(t)}
            </text>
          </g>
        ))}
        <g className="area-layers">
          {n > 0 ? (
            <>
              <path d={layer(c3, c2)} fill={`url(#${uid}-warn)`} />
              <path d={layer(c2, c1)} fill={`url(#${uid}-err)`} />
              <path d={layer(c1, null)} fill={`url(#${uid}-ok)`} />
              <path d={smooth(top(c1))} className="area-line line-ok" />
              <path d={smooth(top(c2))} className="area-line line-err" />
            </>
          ) : null}
        </g>
        {timeline.map((b, i) => (
          <g key={b.start} data-testid="activity-bar">
            <title>{`${fmt(b.start)}: ${b.succeeded} succeeded, ${b.failed} failed, ${b.cancelled} cancelled`}</title>
          </g>
        ))}
        {hb && hover !== null ? (
          <g className="crosshair" pointerEvents="none">
            <line x1={x(hover)} x2={x(hover)} y1={pad.t} y2={H - pad.b} />
            <circle
              cx={x(hover)}
              cy={y(c1[hover])}
              r="4"
              className="dot-ok-fill"
            />
          </g>
        ) : null}
        {labelIdx.map((i, k) =>
          timeline[i] ? (
            <text
              key={k}
              x={x(i)}
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
      {hb && hover !== null ? (
        <div
          className="chart-tip"
          style={{ left: `${(x(hover) / W) * 100}%` }}
          role="status"
        >
          <b>{fmt(hb.start)}</b>
          <span>
            <i className="sw bar-ok" /> {hb.succeeded} succeeded
          </span>
          <span>
            <i className="sw bar-err" /> {hb.failed} failed
          </span>
          {hb.cancelled > 0 ? (
            <span>
              <i className="sw bar-warn" /> {hb.cancelled} cancelled
            </span>
          ) : null}
        </div>
      ) : null}
    </div>
  )
}

/** p50 / p95 / max on one line, to read the latency spread at a glance. */
export function RangeBar({
  p50,
  p95,
  max,
}: {
  p50: number
  p95: number
  max: number
}) {
  const top = Math.max(max, p95, 1)
  const pos = (v: number) => `${Math.min(100, (v / top) * 100)}%`
  return (
    <div
      className="range"
      role="img"
      aria-label="Latency spread: median, p95 and maximum"
    >
      <div className="range-track">
        <span className="range-fill" style={{ width: pos(p95) }} />
        <i className="range-mark" style={{ left: pos(p50) }} />
        <i className="range-mark range-mark-hi" style={{ left: pos(p95) }} />
      </div>
    </div>
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
