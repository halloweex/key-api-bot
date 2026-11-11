import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { render } from '@testing-library/react'
import { StatCard } from '../StatCard'

// jsdom lays nothing out, so widths are written in: every row is 120 px
// wide and every glyph 12 px, which makes 10 characters the most a card holds.
const original = {
  offsetWidth: Object.getOwnPropertyDescriptor(HTMLElement.prototype, 'offsetWidth'),
  clientWidth: Object.getOwnPropertyDescriptor(Element.prototype, 'clientWidth'),
}

beforeEach(() => {
  Object.defineProperty(HTMLElement.prototype, 'offsetWidth', {
    configurable: true,
    get(this: HTMLElement) { return (this.textContent ?? '').length * 12 },
  })
  Object.defineProperty(Element.prototype, 'clientWidth', {
    configurable: true,
    get() { return 120 },
  })
})

afterEach(() => {
  if (original.offsetWidth) Object.defineProperty(HTMLElement.prototype, 'offsetWidth', original.offsetWidth)
  if (original.clientWidth) Object.defineProperty(Element.prototype, 'clientWidth', original.clientWidth)
})

const full = (v: number) => `₴${v.toLocaleString('en-US')}`
const short = (v: number) => (v >= 1e6 ? `₴${Math.round(v / 1e6)}M` : full(v))

const shown = (container: HTMLElement) => container.querySelector('[aria-live="polite"]')

describe('a figure in a card', () => {
  it('is shortened when the whole one does not fit, and kept whole in its title', () => {
    const { container } = render(
      <StatCard label="Revenue" value={108_007_791} formatter={full} shortFormatter={short} />)
    expect(shown(container)?.textContent).toBe('₴108M')
    expect(shown(container)?.getAttribute('title')).toBe('₴108,007,791')
    expect(container.querySelector('article')?.getAttribute('aria-label')).toBe('Revenue: ₴108,007,791')
  })

  it('is whole when it fits', () => {
    const { container } = render(
      <StatCard label="Revenue" value={585_704} formatter={full} shortFormatter={short} />)
    expect(shown(container)?.textContent).toBe('₴585,704')
    expect(shown(container)?.hasAttribute('title')).toBe(false)
  })

  it('counts the trend beside it as taken room', () => {
    // '₴9,876,543' is 10 characters, 120 px: it fits alone, not beside a trend.
    const alone = render(
      <StatCard label="R" value={9_876_543} formatter={full} shortFormatter={short} />)
    expect(shown(alone.container)?.textContent).toBe('₴9,876,543')
    alone.unmount()
    const beside = render(
      <StatCard label="R" value={9_876_543} formatter={full} shortFormatter={short} trend={1} trendValue={4.2} />)
    expect(shown(beside.container)?.textContent).toBe('₴10M')
  })

  it('is never shortened without a short formatter', () => {
    const { container } = render(<StatCard label="Revenue" value={108_007_791} formatter={full} />)
    expect(shown(container)?.textContent).toBe('₴108,007,791')
    expect(container.querySelectorAll('[aria-hidden="true"]').length).toBe(0)
  })
})
