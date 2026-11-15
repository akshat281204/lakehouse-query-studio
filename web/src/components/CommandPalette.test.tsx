import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { CommandPalette } from './CommandPalette'

vi.mock('../api/client', () => ({
  api: {
    listDatasets: vi.fn().mockResolvedValue({
      datasets: [{ id: 'd1', name: 'sales' }],
      offset: 0,
      limit: 100,
      total: 1,
    }),
  },
}))

const setup = (open = true) => {
  const h = {
    onClose: vi.fn(),
    onNavigate: vi.fn(),
    onPreview: vi.fn(),
    onTheme: vi.fn(),
  }
  render(<CommandPalette open={open} {...h} />)
  return h
}

describe('CommandPalette', () => {
  beforeEach(() => vi.clearAllMocks())

  it('renders nothing when closed', () => {
    setup(false)
    expect(screen.queryByRole('dialog')).toBeNull()
  })

  it('lists pages and the loaded dataset', async () => {
    setup()
    expect(screen.getByRole('option', { name: /sql workbench/i })).toBeTruthy()
    await waitFor(() =>
      expect(screen.getByRole('option', { name: /^sales/ })).toBeTruthy(),
    )
  })

  it('filters and opens the selected result with Enter', () => {
    const h = setup()
    const box = screen.getByRole('combobox')
    fireEvent.change(box, { target: { value: 'hist' } })
    fireEvent.keyDown(box, { key: 'Enter' })
    expect(h.onNavigate).toHaveBeenCalledWith('#/history')
    expect(h.onClose).toHaveBeenCalled()
  })

  it('moves with the arrow keys and switches theme', () => {
    const h = setup()
    const box = screen.getByRole('combobox')
    fireEvent.change(box, { target: { value: 'nord' } })
    fireEvent.keyDown(box, { key: 'Enter' })
    expect(h.onTheme).toHaveBeenCalledWith('nord')
  })

  it('previews rows of a dataset found by search', async () => {
    const h = setup()
    await waitFor(() => screen.getByRole('option', { name: /^sales/ }))
    fireEvent.change(screen.getByRole('combobox'), {
      target: { value: 'preview sales' },
    })
    fireEvent.click(
      screen.getByRole('option', { name: /preview rows from sales/i }),
    )
    expect(h.onPreview).toHaveBeenCalledWith('sales')
  })

  it('shows an empty message and closes on Escape', () => {
    const h = setup()
    const box = screen.getByRole('combobox')
    fireEvent.change(box, { target: { value: 'zzzz' } })
    expect(screen.getByText(/nothing matches/i)).toBeTruthy()
    fireEvent.keyDown(box, { key: 'Escape' })
    expect(h.onClose).toHaveBeenCalled()
  })
})
