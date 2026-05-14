import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Checkbox } from '../Checkbox'

describe('Checkbox', () => {
  it('emits boolean on toggle (not event)', async () => {
    const onChange = vi.fn()
    render(<Checkbox checked={false} onChange={onChange} aria-label="agree" />)

    await userEvent.click(screen.getByLabelText('agree'))

    expect(onChange).toHaveBeenCalledTimes(1)
    expect(onChange).toHaveBeenCalledWith(true)
  })

  it('emits false when unchecking', async () => {
    const onChange = vi.fn()
    render(<Checkbox checked={true} onChange={onChange} aria-label="agree" />)

    await userEvent.click(screen.getByLabelText('agree'))

    expect(onChange).toHaveBeenCalledWith(false)
  })

  it('reflects controlled checked prop', () => {
    const { rerender } = render(<Checkbox checked={false} onChange={() => {}} aria-label="x" />)
    expect((screen.getByLabelText('x') as HTMLInputElement).checked).toBe(false)

    rerender(<Checkbox checked={true} onChange={() => {}} aria-label="x" />)
    expect((screen.getByLabelText('x') as HTMLInputElement).checked).toBe(true)
  })

  it('disabled prop is forwarded', () => {
    render(<Checkbox checked={false} onChange={() => {}} disabled aria-label="x" />)
    expect((screen.getByLabelText('x') as HTMLInputElement).disabled).toBe(true)
  })
})
