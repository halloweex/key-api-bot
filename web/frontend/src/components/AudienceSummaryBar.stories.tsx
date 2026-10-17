import type { Meta, StoryObj } from '@storybook/react-vite'
import { AudienceSummaryBar } from './AudienceSummaryBar'
import { Wrapper } from './Wrapper'

/**
 * The audience in one sentence, how many people it is, and the way forward —
 * sticky at the bottom of the screen on a phone, at the foot of the step on
 * desktop.
 *
 * The sentence is generated from the definition, never typed, so it says
 * exactly what the count counted. While something holds the step, the button
 * is disabled and the bar says why in place of the counts — a greyed-out
 * button is never the only clue.
 *
 * The figures are the owner's audience of 2026-10-17: 2 612 people in group 1
 * (1 025 only there), 3 076 in group 2 (1 489 only there), so 1 587 in both
 * and 4 101 in all; a 10% control leaves 3 691 to message.
 */

const OWNER_SENTENCE =
  'Customers with 2+ orders from Jan 1 to Oct 14, 2026, or 1+ order from Jul 16 to Oct 14, 2026 — 4,101 people'

const meta = {
  title: 'Audience builder/AudienceSummaryBar',
  component: AudienceSummaryBar,
  args: {
    sentence: OWNER_SENTENCE,
    stats: '410 of them in the control group · 1,587 match more than one group',
    action: { label: 'Next · 3,691 recipients', onClick: () => {} },
  },
} satisfies Meta<typeof AudienceSummaryBar>

export default meta
type Story = StoryObj<typeof meta>

/** Counted, nothing in the way. */
export const Normal: Story = {}

/**
 * An unfinished condition holds the step: the reason replaces the counts and
 * the button is disabled.
 */
export const Blocked: Story = {
  args: {
    sentence: 'Customers with 2+ orders from Jan 1 to Oct 14, 2026 — 2,800 people',
    blocked: 'Finish the marked condition in group 2 — it is not counted yet.',
    action: { label: 'Next · 2,612 recipients', onClick: () => {}, disabled: true },
  },
}

/** An audience that selects nobody is a block too, with what to do about it. */
export const EmptyAudience: Story = {
  args: {
    sentence: 'Customers with 92+ orders from Jan 1 to Oct 14, 2026 — 0 people',
    blocked: 'Nobody matches this audience yet — loosen a condition.',
    action: { label: 'Next', onClick: () => {}, disabled: true },
  },
}

/** A preview on its way: the sentence is already right, the numbers are not in yet. */
export const Counting: Story = {
  args: {
    sentence: 'Customers with 2+ orders from Jan 1 to Oct 14, 2026, or 1+ order from Jul 16 to Oct 14, 2026',
    stats: 'Counting…',
    action: { label: 'Next', onClick: () => {}, disabled: true },
  },
}

/**
 * On a phone the bar sticks to the bottom of the screen, under the thumb, and
 * the button takes the full width. Scroll the filler to see it stay.
 */
export const StickyOnAPhone: Story = {
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  render: (args) => (
    <Wrapper dir="column" gap="md">
      {Array.from({ length: 14 }, (_, i) => (
        <p key={i} className="text-sm text-slate-500">
          Group card {i + 1} would be here — the conditions scroll under the bar.
        </p>
      ))}
      <AudienceSummaryBar {...args} />
    </Wrapper>
  ),
}
