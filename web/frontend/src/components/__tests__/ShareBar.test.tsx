import { describe, it, expect } from 'vitest'
import { render } from '@testing-library/react'
import { ShareBar } from '../ShareBar'

// The bar is decorative (aria-hidden); the share is the text beside it.
function parts(container: HTMLElement) {
  const track = container.querySelector('[aria-hidden="true"]') as HTMLElement
  return { track, fill: track.firstElementChild as HTMLElement | null }
}

describe('ShareBar', () => {
  it('starts a small share at the start of its track, at least 3 px wide', () => {
    const { container, getByText } = render(<ShareBar pct={0.4} color="#A855F7" />)
    const { fill } = parts(container)
    expect(fill).not.toBeNull()
    expect(fill!.style.width).toBe('0.4%')
    expect(fill!.className).toContain('left-0')
    expect(fill!.className).toContain('min-w-[3px]')
    expect(getByText('0.4%')).toBeTruthy()
  })

  // A track rounded at both ends clipped a fraction of a pixel into a dot at
  // its tip (2026-11-05): the start of the track must be square.
  it('rounds the track only where the share ends', () => {
    const { container } = render(<ShareBar pct={23.8} color="#EC4899" />)
    const { track, fill } = parts(container)
    expect(track.className).toContain('rounded-r')
    expect(track.className).not.toContain('rounded-full')
    expect(fill!.className).toContain('rounded-r')
    expect(fill!.className).not.toContain('rounded-full')
  })

  it('draws nothing for a zero share and still writes it', () => {
    const { container, getByText } = render(<ShareBar pct={0} color="#93C5FD" />)
    expect(parts(container).fill).toBeNull()
    expect(getByText('0.0%')).toBeTruthy()
  })

  it('draws nothing below zero', () => {
    const { container } = render(<ShareBar pct={-2} color="#93C5FD" />)
    expect(parts(container).fill).toBeNull()
  })

  it('clamps the drawing at the whole and writes the share as given', () => {
    const { container, getByText } = render(<ShareBar pct={130} color="#10B981" />)
    expect(parts(container).fill!.style.width).toBe('100%')
    expect(getByText('130.0%')).toBeTruthy()
  })

  it('paints the series colour', () => {
    const { container } = render(<ShareBar pct={50} color="#A855F7" />)
    expect(parts(container).fill!.style.backgroundColor).toBe('rgb(168, 85, 247)')
  })
})
