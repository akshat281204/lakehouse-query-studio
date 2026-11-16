import type { CellValue, ResultColumn } from '../api/types'

export interface ChartPoint {
  label: string
  value: number
}

const NUMERIC = new Set(['int64', 'double'])

export function isNumericColumn(c: ResultColumn): boolean {
  return NUMERIC.has(c.type)
}

/** Columns usable as the value axis. */
export function numericColumns(columns: ResultColumn[]): number[] {
  return columns.flatMap((c, i) => (isNumericColumn(c) ? [i] : []))
}

/**
 * Builds chart points from the loaded rows. Null or non-finite values are
 * skipped and counted so the chart can say how many rows it left out.
 */
export function chartPoints(
  rows: CellValue[][],
  labelIdx: number,
  valueIdx: number,
  max = 50,
): { points: ChartPoint[]; skipped: number; clipped: boolean } {
  const points: ChartPoint[] = []
  let skipped = 0
  for (const row of rows) {
    const v = row[valueIdx]
    if (typeof v !== 'number' || !Number.isFinite(v)) {
      skipped++
      continue
    }
    const l = row[labelIdx]
    points.push({ label: l === null ? '(null)' : String(l), value: v })
  }
  const clipped = points.length > max
  return { points: points.slice(0, max), skipped, clipped }
}
