import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { BadgeSelect } from '../BadgeSelect'

describe('BadgeSelect', () => {
  it('applies tone class based on prop', () => {
    const { rerender, container } = render(
      <BadgeSelect
        options={[{ value: 'a', label: 'A' }]}
        value="a"
        onChange={() => {}}
        tone="purple"
        aria-label="role"
      />,
    )
    expect(container.querySelector('select')?.className).toContain('bg-purple-100')
    expect(container.querySelector('select')?.className).toContain('text-purple-700')

    rerender(
      <BadgeSelect
        options={[{ value: 'a', label: 'A' }]}
        value="a"
        onChange={() => {}}
        tone="red"
        aria-label="role"
      />,
    )
    expect(container.querySelector('select')?.className).toContain('bg-red-100')
  })

  it('emits string on change', async () => {
    const onChange = vi.fn()
    render(
      <BadgeSelect
        options={[
          { value: 'a', label: 'Alpha' },
          { value: 'b', label: 'Beta' },
        ]}
        value="a"
        onChange={onChange}
        tone="slate"
        aria-label="role"
      />,
    )
    await userEvent.selectOptions(screen.getByLabelText('role'), 'b')
    expect(onChange).toHaveBeenCalledWith('b')
  })
})
