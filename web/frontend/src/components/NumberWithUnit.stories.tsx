import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { NumberWithUnit } from './NumberWithUnit'
import { Wrapper } from './Wrapper'

/**
 * A whole number with its unit said beside it. A bare box is how 92 days
 * became 92 orders: «at least [92]» names no unit, so nothing on the screen
 * said which one was being typed.
 *
 * Type into these to see the behaviour that matters: the value is clamped
 * once, on blur, not on every keystroke (try 5000 into the days box — it
 * stays 5000 while you look at it, and becomes 3650 when you leave); and a
 * phone opens its number pad, not the full keyboard.
 */

function Live({
  initial, unit, ...rest
}: Omit<Parameters<typeof NumberWithUnit>[0], 'value' | 'onChange' | 'unit'> & {
  initial: number | null
  /** The unit in the right form for the value — the caller's job. */
  unit?: (value: number | null) => string
}) {
  const [value, setValue] = useState<number | null>(initial)
  return (
    <Wrapper dir="column" gap="xs" align="start">
      <NumberWithUnit {...rest} value={value} onChange={setValue} unit={unit?.(value)} />
      <span className="text-xs text-slate-500 tabular-nums">
        value: {value == null ? 'null' : value}
      </span>
    </Wrapper>
  )
}

const days = (n: number | null) => (n === 1 ? 'day' : 'days')
const orders = (n: number | null) => (n === 1 ? 'order' : 'orders')

const meta = {
  title: 'Forms/NumberWithUnit',
  component: NumberWithUnit,
  // Required props; every story renders through `Live`.
  args: { value: null, onChange: () => {}, ariaLabel: 'Number' },
} satisfies Meta<typeof NumberWithUnit>

export default meta
type Story = StoryObj<typeof meta>

/** A number of days, between 1 and the server's 3 650. */
export const Plain: Story = {
  render: () => (
    <Live initial={90} min={1} max={3650} unit={days} ariaLabel="Number of days" />
  ),
}

/**
 * Small counts get − and +, 36px targets on a phone. The minus is disabled at
 * the minimum; the unit follows the number ("1 order", "2 orders").
 */
export const WithStepper: Story = {
  render: () => (
    <Live
      initial={2}
      min={1}
      max={1000}
      stepper
      unit={orders}
      ariaLabel="Number of orders"
      decreaseLabel="Fewer"
      increaseLabel="More"
    />
  ),
}

/** Money: the currency sign inside the box, no steppers, a placeholder for a typical value. */
export const Hryvnia: Story = {
  render: () => (
    <Wrapper dir="row" gap="sm" align="start" wrap>
      <Live initial={5000} prefix="₴" step={100} placeholder="5000" ariaLabel="From" />
      <Live initial={null} prefix="₴" step={100} placeholder="50000" ariaLabel="To" />
    </Wrapper>
  ),
}

/**
 * The owner's slip, as the box itself sees it: 92 is a valid order count, so
 * the box takes it. What says "did you mean 92 days?" is the row around it
 * (`ConditionRow`'s hint) — the box passes every complete number up as it is
 * typed so that hint can appear while the person is still looking.
 */
export const TheNumberThatWasDays: Story = {
  render: () => (
    <Live initial={92} min={1} max={1000} stepper unit={orders} ariaLabel="Number of orders" />
  ),
}

/** Empty, with its placeholder: `null` is "not filled in", never 0. */
export const Empty: Story = {
  render: () => (
    <Live initial={null} min={1} max={3650} placeholder="90" unit={days} ariaLabel="Number of days" />
  ),
}
