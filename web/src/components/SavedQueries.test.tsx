import { describe, expect, it, vi, beforeEach } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { SavedQueries } from './SavedQueries'
import { SaveQueryForm } from './SaveQueryForm'
import type { SavedQuery } from '../api/types'

const q = (id: string, name: string): SavedQuery => ({
  id,
  name,
  description: `${name} description`,
  statement: `SELECT '${name}'`,
  createdAt: '2024-01-01T00:00:00Z',
  updatedAt: '2024-01-01T00:00:00Z',
})

describe('SavedQueries', () => {
  const onLoad = vi.fn()
  const onDelete = vi.fn()
  beforeEach(() => {
    onLoad.mockReset()
    onDelete.mockReset().mockResolvedValue(undefined)
  })

  it('shows an empty state, a loading state and an error', () => {
    const { rerender } = render(
      <SavedQueries
        items={null}
        error={null}
        onLoad={onLoad}
        onDelete={onDelete}
      />,
    )
    expect(screen.getByText('Loading…')).toBeInTheDocument()
    rerender(
      <SavedQueries
        items={[]}
        error={null}
        onLoad={onLoad}
        onDelete={onDelete}
      />,
    )
    expect(screen.getByText(/nothing saved yet/i)).toBeInTheDocument()
    rerender(
      <SavedQueries
        items={null}
        error="catalog down"
        onLoad={onLoad}
        onDelete={onDelete}
      />,
    )
    expect(screen.getByRole('alert')).toHaveTextContent('catalog down')
  })

  it('loads a query when its name is clicked and marks the open one', () => {
    render(
      <SavedQueries
        items={[q('1', 'Revenue'), q('2', 'Orders')]}
        error={null}
        activeId="2"
        onLoad={onLoad}
        onDelete={onDelete}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'Revenue' }))
    expect(onLoad).toHaveBeenCalledWith(expect.objectContaining({ id: '1' }))
    expect(screen.getByRole('button', { name: 'Orders' })).toHaveAttribute(
      'aria-current',
      'true',
    )
  })

  it('needs a second click to delete and can be cancelled', async () => {
    render(
      <SavedQueries
        items={[q('1', 'Revenue')]}
        error={null}
        onLoad={onLoad}
        onDelete={onDelete}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'Delete Revenue' }))
    expect(onDelete).not.toHaveBeenCalled() // one click is not enough
    fireEvent.click(screen.getByRole('button', { name: 'Keep' }))
    expect(screen.queryByRole('button', { name: 'Keep' })).toBeNull()
    expect(onDelete).not.toHaveBeenCalled()

    fireEvent.click(screen.getByRole('button', { name: 'Delete Revenue' }))
    fireEvent.click(screen.getByRole('button', { name: 'Delete' }))
    await waitFor(() =>
      expect(onDelete).toHaveBeenCalledWith(
        expect.objectContaining({ id: '1' }),
      ),
    )
  })

  it('shows why a delete failed and keeps the confirmation open', async () => {
    onDelete.mockRejectedValue(new Error('catalog unavailable'))
    render(
      <SavedQueries
        items={[q('1', 'Revenue')]}
        error={null}
        onLoad={onLoad}
        onDelete={onDelete}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'Delete Revenue' }))
    fireEvent.click(screen.getByRole('button', { name: 'Delete' }))
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'catalog unavailable',
    )
    expect(screen.getByRole('button', { name: 'Keep' })).toBeInTheDocument()
  })
})

describe('SaveQueryForm', () => {
  it('saves a new query with a trimmed-name guard', () => {
    const onSave = vi.fn()
    render(
      <SaveQueryForm
        busy={false}
        error={null}
        onSave={onSave}
        onCancel={vi.fn()}
      />,
    )
    const save = screen.getByRole('button', { name: 'Save' })
    expect(save).toBeDisabled() // no name yet
    fireEvent.change(screen.getByLabelText(/^Name/), {
      target: { value: '  Revenue  ' },
    })
    fireEvent.change(screen.getByLabelText(/Description/), {
      target: { value: 'by region' },
    })
    fireEvent.click(save)
    expect(onSave).toHaveBeenCalledWith('  Revenue  ', 'by region', true)
  })

  it('offers update and save-as-new for an open saved query', () => {
    const onSave = vi.fn()
    render(
      <SaveQueryForm
        active={q('1', 'Revenue')}
        busy={false}
        error={null}
        onSave={onSave}
        onCancel={vi.fn()}
      />,
    )
    expect(screen.getByRole('button', { name: 'Save as new' })).toBeDisabled() // same name
    fireEvent.click(screen.getByRole('button', { name: 'Update' }))
    expect(onSave).toHaveBeenLastCalledWith(
      'Revenue',
      'Revenue description',
      false,
    )
    fireEvent.change(screen.getByLabelText(/^Name/), {
      target: { value: 'Revenue copy' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Save as new' }))
    expect(onSave).toHaveBeenLastCalledWith(
      'Revenue copy',
      'Revenue description',
      true,
    )
  })

  it('shows the server error and disables the buttons while busy', () => {
    render(
      <SaveQueryForm
        busy
        error="a saved query with this name exists"
        onSave={vi.fn()}
        onCancel={vi.fn()}
      />,
    )
    expect(screen.getByRole('alert')).toHaveTextContent(
      'already'.replace('already', 'a saved query'),
    )
    fireEvent.change(screen.getByLabelText(/^Name/), { target: { value: 'x' } })
    expect(screen.getByRole('button', { name: 'Save' })).toBeDisabled()
  })
})
