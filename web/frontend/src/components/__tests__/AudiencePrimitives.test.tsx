import { describe, it, expect, vi } from 'vitest'
import { useState } from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { NumberWithUnit } from '../NumberWithUnit'
import { SegmentedControl } from '../SegmentedControl'
import { DateRangeField } from '../DateRangeField'
import { MultiPick } from '../MultiPick'

function LiveNumber(props: { min?: number; max?: number; stepper?: boolean; onValue?: (v: number | null) => void }) {
  const [value, setValue] = useState<number | null>(2)
  return (
    <NumberWithUnit
      ariaLabel="count" unit="orders" value={value} stepper={props.stepper}
      min={props.min} max={props.max}
      onChange={(v) => { setValue(v); props.onValue?.(v) }}
    />
  )
}

describe('NumberWithUnit', () => {
  it('passes every complete number up as it is typed, and clamps once, on blur', async () => {
    const seen: Array<number | null> = []
    render(<LiveNumber min={1} max={100} onValue={(v) => seen.push(v)} />)
    const field = screen.getByLabelText('count')
    await userEvent.clear(field)
    await userEvent.type(field, '365')
    // 3 → 36 → 365: never 3 → 30 → 306 under the cursor.
    expect(seen).toEqual([null, 3, 36, 365])
    fireEvent.blur(field)
    expect((field as HTMLInputElement).value).toBe('100')
  })

  it('takes digits only, and opens the number pad', async () => {
    render(<LiveNumber />)
    const field = screen.getByLabelText('count')
    expect(field.getAttribute('inputmode')).toBe('numeric')
    await userEvent.clear(field)
    await userEvent.type(field, '9a2')
    expect((field as HTMLInputElement).value).toBe('92')
  })

  it('steps within its bounds', async () => {
    render(<LiveNumber stepper min={1} max={3} />)
    await userEvent.click(screen.getByRole('button', { name: '+' }))
    await userEvent.click(screen.getByRole('button', { name: '+' }))
    expect((screen.getByLabelText('count') as HTMLInputElement).value).toBe('3')
    expect((screen.getByRole('button', { name: '+' }) as HTMLButtonElement).disabled).toBe(true)
  })
})

describe('SegmentedControl', () => {
  const options = [
    { value: 'all', label: 'All' }, { value: 'last', label: 'Last' },
    { value: 'range', label: 'Range', disabled: true },
  ]

  it('is a radio group with one tab stop, moved by the arrow keys past disabled ones', () => {
    const onChange = vi.fn()
    render(<SegmentedControl ariaLabel="when" value="last" onChange={onChange} options={options} />)
    const radios = screen.getAllByRole('radio')
    expect(radios.map((r) => r.getAttribute('tabindex'))).toEqual(['-1', '0', '-1'])
    fireEvent.keyDown(radios[1], { key: 'ArrowRight' })
    // "Range" is disabled, so the next is "All", wrapping round.
    expect(onChange).toHaveBeenCalledWith('all')
  })

  it('keeps one tab stop when the value matches none of the options', () => {
    render(<SegmentedControl ariaLabel="when" value="15" onChange={() => {}} options={options} />)
    expect(screen.getAllByRole('radio').map((r) => r.getAttribute('tabindex')))
      .toEqual(['0', '-1', '-1'])
  })
})

describe('DateRangeField', () => {
  function Live({ allowOpen = false }: { allowOpen?: boolean }) {
    const [range, setRange] = useState({ from: '2026-01-01', to: '2026-10-14' })
    return (
      <>
        <DateRangeField
          from={range.from} to={range.to} onChange={setRange} allowOpen={allowOpen}
          fromLabel="from" toLabel="to" swappedNote="swapped"
          describe={(days) => `· ${days} days`}
        />
        <output>{`${range.from}|${range.to}`}</output>
      </>
    )
  }

  it('bounds each end by the other and says how many days it covers', () => {
    render(<Live />)
    expect(screen.getByLabelText('to').getAttribute('min')).toBe('2026-01-01')
    expect(screen.getByLabelText('from').getAttribute('max')).toBe('2026-10-14')
    expect(screen.getByText(/· 287 days/)).toBeTruthy()
  })

  it('swaps a reversed pair instead of keeping a period that selects nobody', () => {
    render(<Live />)
    fireEvent.change(screen.getByLabelText('from'), { target: { value: '2026-12-01' } })
    expect(screen.getByRole('status').textContent).toBe('2026-10-14|2026-12-01')
    expect(screen.getByText('swapped')).toBeTruthy()
  })

  it('keeps an emptied end only where an open end means something', () => {
    const { unmount } = render(<Live />)
    fireEvent.change(screen.getByLabelText('to'), { target: { value: '' } })
    expect(screen.getByRole('status').textContent).toBe('2026-01-01|2026-10-14')
    unmount()
    render(<Live allowOpen />)
    fireEvent.change(screen.getByLabelText('to'), { target: { value: '' } })
    expect(screen.getByRole('status').textContent).toBe('2026-01-01|')
  })
})

describe('MultiPick', () => {
  function Live({ allowCustom = false }: { allowCustom?: boolean }) {
    const [values, setValues] = useState<string[]>([])
    return (
      <MultiPick
        ariaLabel="brand" placeholder="search" emptyText="nothing"
        options={[{ value: 'Anua', label: 'Anua' }, { value: 'COSRX', label: 'COSRX' }]}
        values={values} onChange={setValues} allowCustom={allowCustom}
        addLabel={(v) => `add ${v}`} removeLabel={(l) => `remove ${l}`}
      />
    )
  }

  it('filters as it is searched, toggles, and removes from the chips', async () => {
    render(<Live />)
    await userEvent.type(screen.getByLabelText('brand'), 'an')
    expect(screen.queryByRole('button', { name: 'COSRX' })).toBeNull()
    await userEvent.click(screen.getByRole('button', { name: 'Anua' }))
    expect(screen.getByRole('button', { name: 'Anua' }).getAttribute('aria-pressed')).toBe('true')
    await userEvent.click(screen.getByRole('button', { name: 'remove Anua' }))
    expect(screen.getByRole('button', { name: 'Anua' }).getAttribute('aria-pressed')).toBe('false')
  })

  it('takes a value of its own when there is no list to offer', async () => {
    render(<Live allowCustom />)
    await userEvent.type(screen.getByLabelText('brand'), 'Київ{Enter}')
    expect(screen.getByRole('button', { name: 'remove Київ' })).toBeTruthy()
  })
})
