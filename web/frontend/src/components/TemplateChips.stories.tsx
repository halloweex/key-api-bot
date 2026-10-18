import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { History, Bookmark } from 'lucide-react'
import { Button } from './Button'
import { TemplateChips, type TemplateChip } from './TemplateChips'
import { Wrapper } from './Wrapper'

/**
 * «Where to start?» — the audiences a campaign usually starts from, one tap
 * each. A template is a starting point, not a black box: it fills the groups
 * with ordinary conditions that are then edited like any other, and its hint
 * says what it will put there before it is tapped.
 *
 * A row that scrolls sideways on a phone — nine chips wrapping would push the
 * groups below the fold — and wraps on desktop. `extra` holds the other two
 * ways in: like a past campaign, and a saved audience.
 *
 * Each chip says how many people it would select today, beside its name. The
 * counts are asked one at a time once the row is on screen; «…» while one is
 * on its way, nothing when it could not be had. The figures below are
 * production's on 2026-10-17 (retail).
 */

const TEMPLATES: TemplateChip[] = [
  { key: 'all', label: 'All buyers', hint: '1+ order, all time' },
  { key: 'recent', label: 'Recent', hint: '1+ order in the last 30 days' },
  { key: 'new', label: 'New', hint: 'first order in the last 30 days' },
  { key: 'repeat', label: 'Repeat', hint: '2+ orders, all time' },
  { key: 'oneAndGone', label: 'Bought once, gone', hint: 'exactly 1 order, 60+ days ago' },
  { key: 'churning', label: 'Drifting away', hint: 'last order 90–180 days ago' },
  { key: 'lost', label: 'Lost', hint: 'last order 240+ days ago' },
  { key: 'vip', label: 'VIP', hint: 'value level VIP' },
  { key: 'brand', label: 'Bought a brand…', hint: 'pick the brand' },
]

const COUNTS: Record<string, string> = {
  all: '17,891', recent: '1,185', new: '507', repeat: '6,729', oneAndGone: '10,369',
  churning: '2,662', lost: '10,728', vip: '2,041',
}

function Live({
  withExtra = false, counts,
}: { withExtra?: boolean; counts?: Record<string, string> }) {
  const [picked, setPicked] = useState<string | null>(null)
  return (
    <Wrapper dir="column" gap="sm">
      <TemplateChips
        title="Where to start?"
        items={TEMPLATES.map((t) => ({ ...t, count: counts?.[t.key] }))}
        onPick={setPicked}
        extra={withExtra ? (
          <Wrapper dir="row" gap="sm" wrap>
            <Button variant="secondary" size="sm">
              <History className="h-3.5 w-3.5" aria-hidden /> Like a past campaign…
            </Button>
            <Button variant="ghost" size="sm">
              <Bookmark className="h-3.5 w-3.5" aria-hidden /> Saved audiences
            </Button>
          </Wrapper>
        ) : undefined}
      />
      <span className="text-xs text-slate-500">
        {picked ? `Picked: ${picked}` : 'Nothing picked yet.'}
      </span>
    </Wrapper>
  )
}

const meta = {
  title: 'Audience builder/TemplateChips',
  component: TemplateChips,
  // Required props; every story renders through `Live`.
  args: { title: 'Where to start?', items: TEMPLATES, onPick: () => {} },
} satisfies Meta<typeof TemplateChips>

export default meta
type Story = StoryObj<typeof meta>

/** The nine templates, wrapping on desktop. */
export const Default: Story = {
  render: () => <Live />,
}

/** With the library's two ways in after the chips, as the wizard draws it. */
export const WithLibrary: Story = {
  render: () => <Live withExtra />,
}

/** Each chip with how many it would select today; "a brand…" has no count
 *  until the brand is picked. */
export const WithCounts: Story = {
  render: () => <Live withExtra counts={COUNTS} />,
}

/** The counts arrive one at a time: the first two in, the rest on their way,
 *  one that failed showing nothing. */
export const CountsArriving: Story = {
  render: () => (
    <Live counts={{ all: '17,891', recent: '1,185', new: '…', repeat: '…', churning: '…',
                    lost: '…', vip: '…' }} />
  ),
}

/** A phone: one row that scrolls sideways, each chip a 44px target. */
export const ScrollingOnAPhone: Story = {
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  render: () => <Live withExtra counts={COUNTS} />,
}
