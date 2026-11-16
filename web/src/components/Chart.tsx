import { useState } from 'react'
import type { CellValue, ResultColumn } from '../api/types'
import { chartPoints, numericColumns } from '../lib/chart'

interface ChartProps {
  columns: ResultColumn[]
  rows: CellValue[][]
}

const W = 640
const H = 240
const PAD = { l: 48, r: 12, t: 12, b: 56 }

export function Chart({ columns, rows }: ChartProps) {
  const numeric = numericColumns(columns)
  const [kind, setKind] = useState<'bar' | 'line'>('bar')
  const [labelIdx, setLabelIdx] = useState(0)
  const [valueIdx, setValueIdx] = useState(numeric[0] ?? -1)

  if (numeric.length === 0) {
    return (
      <p className="note">
        Charts need at least one numeric column in the result.
      </p>
    )
  }
  const { points, skipped, clipped } = chartPoints(rows, labelIdx, valueIdx)
  const max = Math.max(0, ...points.map((p) => p.value))
  const min = Math.min(0, ...points.map((p) => p.value))
  const span = max - min || 1
  const y = (v: number) => PAD.t + (H - PAD.t - PAD.b) * (1 - (v - min) / span)
  const step = (W - PAD.l - PAD.r) / Math.max(points.length, 1)
  const x = (i: number) => PAD.l + step * i + step / 2

  return (
    <div className="chart">
      <div className="chart-controls">
        <label>
          Chart type{' '}
          <select
            value={kind}
            onChange={(e) => setKind(e.target.value as 'bar' | 'line')}
          >
            <option value="bar">Bar</option>
            <option value="line">Line</option>
          </select>
        </label>
        <label>
          Label{' '}
          <select
            value={labelIdx}
            onChange={(e) => setLabelIdx(Number(e.target.value))}
          >
            {columns.map((c, i) => (
              <option key={i} value={i}>
                {c.name}
              </option>
            ))}
          </select>
        </label>
        <label>
          Value{' '}
          <select
            value={valueIdx}
            onChange={(e) => setValueIdx(Number(e.target.value))}
          >
            {numeric.map((i) => (
              <option key={i} value={i}>
                {columns[i].name}
              </option>
            ))}
          </select>
        </label>
      </div>
      {points.length === 0 ? (
        <p className="note">No numeric values to plot in the loaded rows.</p>
      ) : (
        <svg
          viewBox={`0 0 ${W} ${H}`}
          role="img"
          aria-label={`${kind} chart of ${columns[valueIdx].name} by ${columns[labelIdx].name}`}
        >
          <line
            x1={PAD.l}
            y1={y(0)}
            x2={W - PAD.r}
            y2={y(0)}
            stroke="currentColor"
            opacity="0.4"
          />
          <text x={4} y={y(max) + 4} fontSize="10" fill="currentColor">
            {max}
          </text>
          <text x={4} y={y(min) + 4} fontSize="10" fill="currentColor">
            {min}
          </text>
          {kind === 'bar' ? (
            points.map((p, i) => (
              <rect
                key={i}
                data-testid="chart-mark"
                x={x(i) - step * 0.35}
                width={step * 0.7}
                y={Math.min(y(p.value), y(0))}
                height={Math.abs(y(p.value) - y(0))}
                fill="#2563eb"
              >
                <title>{`${p.label}: ${p.value}`}</title>
              </rect>
            ))
          ) : (
            <>
              <polyline
                fill="none"
                stroke="#2563eb"
                strokeWidth="2"
                points={points.map((p, i) => `${x(i)},${y(p.value)}`).join(' ')}
              />
              {points.map((p, i) => (
                <circle
                  key={i}
                  data-testid="chart-mark"
                  cx={x(i)}
                  cy={y(p.value)}
                  r="3"
                  fill="#2563eb"
                >
                  <title>{`${p.label}: ${p.value}`}</title>
                </circle>
              ))}
            </>
          )}
          {points.length <= 12
            ? points.map((p, i) => (
                <text
                  key={i}
                  x={x(i)}
                  y={H - PAD.b + 14}
                  fontSize="10"
                  textAnchor="end"
                  fill="currentColor"
                  transform={`rotate(-30 ${x(i)} ${H - PAD.b + 14})`}
                >
                  {p.label.length > 14 ? `${p.label.slice(0, 13)}…` : p.label}
                </text>
              ))
            : null}
        </svg>
      )}
      <p className="note">
        {skipped > 0
          ? `${skipped} row(s) without a numeric value were left out. `
          : ''}
        {clipped ? 'Showing the first 50 points. ' : ''}
        Chart uses only the rows loaded on this page.
      </p>
    </div>
  )
}
