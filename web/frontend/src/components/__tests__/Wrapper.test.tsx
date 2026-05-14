import { describe, it, expect } from 'vitest'
import { render } from '@testing-library/react'
import { Wrapper } from '../Wrapper'

describe('Wrapper', () => {
  it('maps semantic props to Tailwind tokens (sanity)', () => {
    const { container } = render(
      <Wrapper dir="row" gap="md" align="center" justify="between" padding="lg">
        <span>x</span>
      </Wrapper>,
    )
    const el = container.firstChild as HTMLElement
    expect(el.className).toContain('flex')
    expect(el.className).toContain('flex-row')
    expect(el.className).toContain('gap-3')
    expect(el.className).toContain('items-center')
    expect(el.className).toContain('justify-between')
    expect(el.className).toContain('p-4')
  })

  it('row-responsive maps to flex-col sm:flex-row', () => {
    const { container } = render(
      <Wrapper dir="row-responsive">
        <span>x</span>
      </Wrapper>,
    )
    expect((container.firstChild as HTMLElement).className).toContain('flex-col')
    expect((container.firstChild as HTMLElement).className).toContain('sm:flex-row')
  })

  it('renders as alternate element when `as` is passed', () => {
    const { container } = render(
      <Wrapper as="section">
        <span>x</span>
      </Wrapper>,
    )
    expect(container.firstChild?.nodeName).toBe('SECTION')
  })

  it('does not accept className/style (compile-time check via type — runtime: no leak)', () => {
    const { container } = render(
      <Wrapper>
        <span>x</span>
      </Wrapper>,
    )
    // Whatever classes are present, they come from props — never an
    // arbitrary string from a consumer.
    expect((container.firstChild as HTMLElement).getAttribute('style')).toBeNull()
  })
})
