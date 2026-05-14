import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Input } from '../Input'

describe('Input', () => {
  it('emits raw string value (not event) on change', async () => {
    const onChange = vi.fn()
    render(<Input value="" onChange={onChange} placeholder="search" />)

    await userEvent.type(screen.getByPlaceholderText('search'), 'abc')

    // userEvent types one char at a time
    expect(onChange).toHaveBeenCalledTimes(3)
    expect(onChange).toHaveBeenNthCalledWith(1, 'a')
    expect(onChange).toHaveBeenNthCalledWith(2, 'b')
    expect(onChange).toHaveBeenNthCalledWith(3, 'c')
  })

  it('renders prefix when provided', () => {
    render(<Input value="50" onChange={() => {}} prefix="₴" />)
    expect(screen.getByText('₴')).toBeInTheDocument()
  })

  it('does not wrap with prefix container when prefix absent', () => {
    const { container } = render(<Input value="" onChange={() => {}} />)
    // Without prefix the wrapper div is absent — only the bare <input>
    expect(container.firstChild?.nodeName).toBe('INPUT')
  })

  it('forwards behavioural HTML attrs (disabled, min, max)', () => {
    render(
      <Input type="number" value={5} onChange={() => {}} min={1} max={10} step={1} disabled />,
    )
    const el = screen.getByRole('spinbutton') as HTMLInputElement
    expect(el.disabled).toBe(true)
    expect(el.min).toBe('1')
    expect(el.max).toBe('10')
    expect(el.step).toBe('1')
  })

  it('width prop maps to layout token (not raw CSS)', () => {
    const { rerender, container } = render(
      <Input value="" onChange={() => {}} width="full" />,
    )
    expect(container.querySelector('input')?.className).toContain('w-full')

    rerender(<Input value="" onChange={() => {}} width="narrow" />)
    expect(container.querySelector('input')?.className).toContain('w-20')
  })
})
