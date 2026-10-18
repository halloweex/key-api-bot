import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { SegmentedControl, type SegmentOption } from './SegmentedControl'

/**
 * Two to five mutually exclusive choices, all visible at once. It stands where
 * a select would hide the alternatives behind a tap — "All time | Last N days |
 * From … to …" is only useful if the three can be read side by side.
 *
 * A radio group to assistive technology: one tab stop, the arrow keys move the
 * choice (and skip a disabled option).
 */

type Period = 'all' | 'last' | 'range'

const PERIODS: Array<SegmentOption<Period>> = [
  { value: 'all', label: 'All time' },
  { value: 'last', label: 'Last N days' },
  { value: 'range', label: 'From … to …' },
]

const HOLDOUTS: Array<SegmentOption<string>> = ['10', '20', '30', '40']
  .map((pct) => ({ value: pct, label: `${pct}%` }))

/**
 * Stateful: the control is controlled, and a story with a frozen `value` would
 * show segments that cannot be pressed.
 */
function Live<T extends string>({
  options, initial, ariaLabel, size, fullWidth,
}: {
  options: Array<SegmentOption<T>>
  initial: T
  ariaLabel: string
  size?: 'sm' | 'md'
  fullWidth?: boolean
}) {
  const [value, setValue] = useState<T>(initial)
  return (
    <SegmentedControl
      options={options}
      value={value}
      onChange={setValue}
      ariaLabel={ariaLabel}
      size={size}
      fullWidth={fullWidth}
    />
  )
}

const meta = {
  title: 'Forms/SegmentedControl',
  component: SegmentedControl,
  // `options`, `value`, `onChange` and `ariaLabel` are required, and every
  // story renders through `Live`; these args only satisfy the type.
  args: {
    options: PERIODS,
    value: 'all',
    onChange: () => {},
    ariaLabel: 'Period',
  },
} satisfies Meta<typeof SegmentedControl>

export default meta
type Story = StoryObj<typeof meta>

/** The period control's modes — the reason this component exists. */
export const Default: Story = {
  render: () => <Live options={PERIODS} initial="all" ariaLabel="When" />,
}

/**
 * One mode taken by a sibling condition: it stays in place, disabled, rather
 * than vanishing — a control whose options change shape reads as broken. The
 * arrow keys step over it.
 */
export const WithDisabledOption: Story = {
  render: () => (
    <Live
      options={PERIODS.map((o) => (o.value === 'all' ? { ...o, disabled: true } : o))}
      initial="last"
      ariaLabel="When"
    />
  ),
}

/** The small size, as the control-group share uses it in step 2. */
export const Small: Story = {
  render: () => <Live options={HOLDOUTS} initial="10" ariaLabel="Control group" size="sm" />,
}

/**
 * The phone layout: segments share the width equally, each at least 36px
 * tall — the floor for a thumb beside other targets.
 */
export const FullWidthOnAPhone: Story = {
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  render: () => <Live options={PERIODS} initial="range" ariaLabel="When" size="sm" fullWidth />,
}
