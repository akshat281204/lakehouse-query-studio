import { describe, it, expect } from 'vitest'
import { tokenize, relationRef } from './sql'
import { chartPoints, numericColumns } from './chart'
import { formatBytes, formatDuration, isTerminal } from './format'

describe('tokenize', () => {
  it('round-trips any input exactly', () => {
    const inputs = [
      "SELECT a, 'it''s' FROM t -- c\nWHERE x > 1.5 /* b */",
      'SELECT "weird ""name""" FROM t',
      "SELECT 'unterminated",
      '/* open comment',
      '',
      'select\tñ, 日本 from t',
    ]
    for (const s of inputs)
      expect(
        tokenize(s)
          .map((t) => t.text)
          .join(''),
      ).toBe(s)
  })

  it('classifies keywords, strings, numbers and comments', () => {
    const kinds = Object.fromEntries(
      tokenize("select 'x', 42 -- hi")
        .filter((t) => t.kind !== 'text')
        .map((t) => [t.text, t.kind]),
    )
    expect(kinds).toEqual({
      select: 'keyword',
      "'x'": 'string',
      '42': 'number',
      '-- hi': 'comment',
    })
  })

  it('does not treat keywords inside strings or identifiers as keywords', () => {
    expect(tokenize("'select from'").every((t) => t.kind === 'string')).toBe(
      true,
    )
    expect(tokenize('selected').some((t) => t.kind === 'keyword')).toBe(false)
  })
})

describe('relationRef', () => {
  it('quotes names that are not plain identifiers', () => {
    expect(relationRef('sales')).toBe('sales')
    expect(relationRef('web-logs')).toBe('"web-logs"')
    expect(relationRef('1abc')).toBe('"1abc"')
  })
})

describe('chart helpers', () => {
  const cols = [
    { name: 'k', type: 'string' },
    { name: 'v', type: 'int64' },
    { name: 'f', type: 'double' },
  ]
  it('finds numeric columns', () =>
    expect(numericColumns(cols)).toEqual([1, 2]))
  it('skips nulls and counts them', () => {
    const r = chartPoints(
      [
        ['a', 1, 1.5],
        ['b', null, 2],
        ['c', 3, 3],
      ],
      0,
      1,
    )
    expect(r.points).toEqual([
      { label: 'a', value: 1 },
      { label: 'c', value: 3 },
    ])
    expect(r.skipped).toBe(1)
  })
  it('clips to the maximum number of points', () => {
    const rows = Array.from({ length: 80 }, (_, i) => [`r${i}`, i, i])
    const r = chartPoints(rows, 0, 1)
    expect(r.points).toHaveLength(50)
    expect(r.clipped).toBe(true)
  })
  it('labels null categories', () => {
    expect(chartPoints([[null, 1, 1]], 0, 1).points[0].label).toBe('(null)')
  })
})

describe('format', () => {
  it('formats durations and bytes', () => {
    expect(formatDuration(undefined)).toBe('—')
    expect(formatDuration(250)).toBe('250 ms')
    expect(formatDuration(1500)).toBe('1.50 s')
    expect(formatBytes(2048)).toBe('2.0 KiB')
  })
  it('knows terminal states', () => {
    expect(isTerminal('SUCCEEDED')).toBe(true)
    expect(isTerminal('RUNNING')).toBe(false)
  })
})
