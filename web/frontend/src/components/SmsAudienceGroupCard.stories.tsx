import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SmsAudienceGroupCard } from './SmsAudienceGroupCard'
import { emptyGroup } from '../utils/smsAudience'
import type { Brand, Category, SmsAudienceGroup, SmsAudienceGroupShare } from '../types/api'

/**
 * One group of an SMS audience: its own window and its own filters, all of
 * which must hold. The audience is the union of its groups, so each card says
 * what its group brings — and, when it brings nobody, says that instead of a
 * zero nobody can explain.
 *
 * "Orders in a period" is the condition groups were built for: a lifetime
 * order count cannot say "two purchases this year". When the period reaches
 * further back than the group's window, the card warns that the window is
 * cutting it.
 */

const BRANDS: Brand[] = [{ name: 'COSRX' }, { name: 'Anua' }, { name: 'Round Lab' }]
const CATEGORIES: Category[] = [
  { id: 1, name: 'Догляд за обличчям' }, { id: 2, name: 'Очищення' },
]

function client() {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchInterval: false } },
  })
  qc.setQueryData(['brands'], BRANDS)
  qc.setQueryData(['categories'], CATEGORIES)
  return qc
}

/** Stateful, so the controls can be pressed and typed into. */
function Live({
  index = 0, initial, share, removable = false,
}: {
  index?: number
  initial: SmsAudienceGroup
  share?: SmsAudienceGroupShare
  removable?: boolean
}) {
  const [group, setGroup] = useState(initial)
  return (
    <QueryClientProvider client={client()}>
      <SmsAudienceGroupCard
        index={index}
        group={group}
        onChange={setGroup}
        onRemove={removable ? () => {} : undefined}
        share={share}
      />
    </QueryClientProvider>
  )
}

const meta = {
  title: 'SMS/SmsAudienceGroupCard',
  component: SmsAudienceGroupCard,
  parameters: { layout: 'padded' },
  // Required props, satisfied for the type; every story renders through Live.
  args: { index: 0, group: emptyGroup(), onChange: () => {} },
} satisfies Meta<typeof SmsAudienceGroupCard>

export default meta
type Story = StoryObj<typeof meta>

/** A fresh group: everyone inside the default window, nothing else asked. */
export const Empty: Story = {
  render: () => <Live initial={emptyGroup()} />,
}

/** The owner's first group — two purchases since 1 January, in a window that
 *  holds the whole year — beside another, so it reports its share. */
export const TwoPurchasesThisYear: Story = {
  render: () => (
    <Live
      initial={{ maxRecencyDays: 365,
                 filters: { periodOrdersMin: 2, periodFrom: '2026-01-01' } }}
      share={{ group: 1, matched: 2_739, recipients: 2_626, onlyHere: 1_035 }}
    />
  ),
}

/** The period said as a number of days, on a second group that can be
 *  removed. */
export const InTheLastNinetyTwoDays: Story = {
  render: () => (
    <Live
      index={1}
      removable
      initial={{ maxRecencyDays: 270, filters: { periodDays: 92 } }}
      share={{ group: 2, matched: 3_237, recipients: 3_103, onlyHere: 1_512 }}
    />
  ),
}

/** A period reaching back further than the window: the card says the window
 *  is cutting it, rather than letting it quietly redefine the year. */
export const PeriodCutByTheWindow: Story = {
  render: () => (
    <Live initial={{ maxRecencyDays: 120, filters: { periodOrdersMin: 2, periodDays: 300 } }} />
  ),
}

/** Matches people, none of whom can be messaged. */
export const NobodyReachable: Story = {
  render: () => (
    <Live
      index={2}
      removable
      initial={{ maxRecencyDays: 270, filters: { cities: ['Ужгород'] } }}
      share={{ group: 3, matched: 14, recipients: 0, onlyHere: 0 }}
    />
  ),
}
