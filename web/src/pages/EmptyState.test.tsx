import { describe, it, expect } from 'vitest'
import { render, screen } from '@testing-library/react'
import { EmptyState } from './EmptyState'

describe('EmptyState', () => {
  it('renders the heading and empty-state guidance', () => {
    render(<EmptyState />)

    expect(
      screen.getByRole('heading', {
        level: 1,
        name: /lakehouse query studio/i,
      }),
    ).toBeInTheDocument()
    expect(screen.getByText(/no datasets yet/i)).toBeInTheDocument()
  })
})
