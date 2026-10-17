import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { DateRangeField } from './DateRangeField'
import { Wrapper } from './Wrapper'

/**
 * A period as two native date inputs in one frame, then what they mean in
 * words. Native on purpose: on a phone it is the system's own picker, in the
 * reader's locale, with nothing to load.
 *
 * It cannot be reversed: the end's `min` is the start and the start's `max`
 * is the end. A reversed pair typed into a desktop field anyway is swapped,
 * and a line says so.
 */

function Live({
  from, to, allowOpen,
}: { from: string; to: string; allowOpen?: boolean }) {
  const [range, setRange] = useState({ from, to })
  return (
    <Wrapper dir="column" gap="xs">
      <DateRangeField
        from={range.from}
        to={range.to}
        onChange={setRange}
        fromLabel="Start date"
        toLabel="End date, included"
        allowOpen={allowOpen}
        describe={(days) => `· ${days} ${days === 1 ? 'day' : 'days'}`}
        swappedNote="The start was after the end — the dates were swapped."
      />
      <span className="text-xs text-slate-400 tabular-nums">
        from: {range.from || '(open)'} · to: {range.to || '(open)'}
      </span>
    </Wrapper>
  )
}

const meta = {
  title: 'Forms/DateRangeField',
  component: DateRangeField,
  // Required props; every story renders through `Live`.
  args: {
    from: '',
    to: '',
    onChange: () => {},
    fromLabel: 'Start date',
    toLabel: 'End date',
    swappedNote: 'The dates were swapped.',
  },
} satisfies Meta<typeof DateRangeField>

export default meta
type Story = StoryObj<typeof meta>

/**
 * A closed range — the owner's first group, 1 January to 14 October 2026 —
 * with its length under it, both ends included.
 */
export const ClosedRange: Story = {
  render: () => <Live from="2026-01-01" to="2026-10-14" />,
}

/**
 * An open end: "since 1 January". With `allowOpen` an emptied end stays
 * empty, and the caller decides what that means; the length line appears only
 * once both ends are set.
 */
export const OpenEnd: Story = {
  render: () => <Live from="2026-01-01" to="" allowOpen />,
}

/** "Up to 14 October": the other open end. */
export const OpenStart: Story = {
  render: () => <Live from="" to="2026-10-14" allowOpen />,
}

/**
 * Without `allowOpen`, clearing an end puts the previous date back — a
 * period that must have both ends never quietly loses one.
 */
export const BothEndsRequired: Story = {
  render: () => <Live from="2026-07-16" to="2026-10-14" />,
}
