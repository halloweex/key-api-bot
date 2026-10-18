import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { PeriodControl } from './PeriodControl'
import type { PeriodValue } from '../utils/smsConditions'

/**
 * When something is counted, as one control with three ways of saying it:
 * «All time | Last N days | From … to …». It replaced five fields that each
 * said part of this and interacted out of sight — which is how a period that
 * started on 1 January was quietly cut by a 270-day window nobody had set.
 *
 * "The last N days" says which dates those are, under the number, both ends
 * included. Switching modes carries the same days across: last 90 days become
 * those 90 days as dates.
 *
 * `today` is pinned in every story so the resolved dates do not move from one
 * day to the next; the page passes nothing and gets Kyiv's today.
 */

const TODAY = '2026-10-17'

function Live({
  initial, kinds, unavailable, today = TODAY,
}: {
  initial: PeriodValue
  kinds?: Array<PeriodValue['kind']>
  unavailable?: 'disable' | 'hide'
  today?: string
}) {
  const [value, setValue] = useState<PeriodValue>(initial)
  return (
    <PeriodControl
      value={value}
      onChange={setValue}
      kinds={kinds}
      unavailable={unavailable}
      ariaLabel="When"
      today={today}
    />
  )
}

const meta = {
  title: 'Forms/PeriodControl',
  component: PeriodControl,
  // Required props; every story renders through `Live`.
  args: { value: { kind: 'all' }, onChange: () => {}, ariaLabel: 'When' },
} satisfies Meta<typeof PeriodControl>

export default meta
type Story = StoryObj<typeof meta>

/** Over all time: nothing more to fill in. */
export const AllTime: Story = {
  render: () => <Live initial={{ kind: 'all' }} />,
}

/**
 * The last N days, with the quick chips and the dates it resolves to. Kept
 * as N and resolved each time it is sent, so it moves with the calendar.
 */
export const LastNDays: Story = {
  render: () => <Live initial={{ kind: 'last', days: 90 }} />,
}

/**
 * Exactly what the server counts: an order is in "the last N days" when
 * today − its date ≤ N, so on 18 Oct 2026 the last 91 days are 19 Jul to
 * 18 Oct, both included — 92 calendar days. The line under the number says
 * the dates in full, the year once; `tests/unit/test_sms_last_n_days_edge.py`
 * runs the server's predicate for these very dates.
 */
export const LastNDaysIsNPlusOneCalendarDays: Story = {
  render: () => <Live initial={{ kind: 'last', days: 91 }} today="2026-10-18" />,
}

/**
 * Between two dates — the owner's first group, 1 Jan to 14 Oct 2026. Kept as
 * dates: a campaign repeated next month asks about the same days.
 */
export const DateRange: Story = {
  render: () => <Live initial={{ kind: 'range', from: '2026-01-01', to: '2026-10-14' }} />,
}

/** A range with an open end: "since 1 January". */
export const OpenEndedRange: Story = {
  render: () => <Live initial={{ kind: 'range', from: '2026-01-01', to: '' }} />,
}

/**
 * A sibling condition already counts orders over all time, so this one can
 * only be a period: "All time" stays on screen, disabled (`unavailable` is
 * `disable`, the default) — it is taken for now, not impossible.
 */
export const AllTimeTakenBySibling: Story = {
  render: () => <Live initial={{ kind: 'last', days: 90 }} kinds={['last', 'range']} />,
}

/**
 * "Bought within": the server has no date range for a purchase, so the range
 * mode is not shown at all (`unavailable="hide"`) — something this period can
 * never be, rather than something taken.
 */
export const LimitedAndHidden: Story = {
  render: () => <Live initial={{ kind: 'last', days: 180 }} kinds={['all', 'last']} unavailable="hide" />,
}
