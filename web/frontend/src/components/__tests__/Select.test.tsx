import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { Select } from '../Select'

const OPTS = [
  { value: 'a', label: 'Alpha' },
  { value: 'b', label: 'Beta' },
]

describe('Select', () => {
  it('emits null when empty option selected (allowEmpty=true)', async () => {
    const onChange = vi.fn()
    render(
      <Select
        options={OPTS}
        value="a"
        onChange={onChange}
        emptyLabel="All"
        aria-label="picker"
      />,
    )

    await userEvent.selectOptions(screen.getByLabelText('picker'), '')
    expect(onChange).toHaveBeenCalledWith(null)
  })

  it('emits string when concrete option selected', async () => {
    const onChange = vi.fn()
    render(<Select options={OPTS} value={null} onChange={onChange} aria-label="picker" />)

    await userEvent.selectOptions(screen.getByLabelText('picker'), 'b')
    expect(onChange).toHaveBeenCalledWith('b')
  })

  it('does not render empty option when allowEmpty=false', () => {
    render(
      <Select
        options={OPTS}
        value="a"
        onChange={() => {}}
        allowEmpty={false}
        aria-label="picker"
      />,
    )
    expect(screen.queryByText('All')).not.toBeInTheDocument()
  })

  it('variant prop swaps the className token', () => {
    const { rerender, container } = render(
      <Select options={OPTS} value="a" onChange={() => {}} variant="framed" />,
    )
    expect(container.querySelector('select')?.className).toContain('shadow-sm')

    rerender(<Select options={OPTS} value="a" onChange={() => {}} variant="pill" />)
    expect(container.querySelector('select')?.className).toContain('bg-slate-100')
    expect(container.querySelector('select')?.className).not.toContain('shadow-sm')
  })
})
