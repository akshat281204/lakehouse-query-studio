import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { ThemePicker } from './ThemePicker'
import { THEMES, isTheme, loadTheme } from '../lib/theme'

// Node's experimental global localStorage can shadow jsdom's; use a plain one.
function memoryStorage(): Storage {
  const m = new Map<string, string>()
  return {
    get length() {
      return m.size
    },
    clear: () => m.clear(),
    getItem: (k) => m.get(k) ?? null,
    key: (i) => [...m.keys()][i] ?? null,
    removeItem: (k) => void m.delete(k),
    setItem: (k, v) => void m.set(k, String(v)),
  }
}

describe('theme', () => {
  beforeEach(() => {
    vi.stubGlobal('localStorage', memoryStorage())
    delete document.documentElement.dataset.theme
  })
  afterEach(() => vi.unstubAllGlobals())

  it('every theme id is unique and recognised', () => {
    const ids = THEMES.map((t) => t.id)
    expect(new Set(ids).size).toBe(ids.length)
    ids.forEach((id) => expect(isTheme(id)).toBe(true))
    expect(isTheme('nope')).toBe(false)
  })

  it('ignores an unknown saved theme', () => {
    localStorage.setItem('lqs-theme', 'bogus')
    expect(THEMES.map((t) => t.id)).toContain(loadTheme())
  })

  it('switches theme, applies it to <html> and remembers it', () => {
    render(<ThemePicker />)
    fireEvent.click(screen.getByRole('button', { name: /^theme:/i }))
    fireEvent.click(screen.getByRole('menuitemradio', { name: /nord/i }))
    expect(document.documentElement.dataset.theme).toBe('nord')
    expect(localStorage.getItem('lqs-theme')).toBe('nord')
    expect(screen.queryByRole('menu')).toBeNull()
  })

  it('closes on Escape', () => {
    render(<ThemePicker />)
    fireEvent.click(screen.getByRole('button', { name: /^theme:/i }))
    expect(screen.getByRole('menu')).toBeTruthy()
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(screen.queryByRole('menu')).toBeNull()
  })
})
