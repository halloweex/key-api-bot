import type { Meta, StoryObj } from '@storybook/react-vite'
import { SmsAudiencePreview } from './SmsAudiencePreview'
import type { SmsSegmentsResponse } from '../types/api'

/**
 * Who is in the audience right now. One arm states the rule that built it —
 * for an audience of several groups that rule is "anyone in any of them",
 * because the widest group's window describes none of them.
 */

const CRITERIA = {
  ltvBasis: 'margin' as const, vipLtv: 5500, coreLtv: 2750, coreMinOrders: 2,
  reactivationMaxRecency: 120, holdoutPct: 10, grouping: 'single' as const,
}

const FUNNEL: SmsSegmentsResponse['funnel'] = [
  { stage: 'customers', remaining: 22_410 },
  { stage: 'inWindow', remaining: 7_902 },
  { stage: 'filtered', remaining: 4_480 },
  { stage: 'tiered', remaining: 4_480 },
  { stage: 'phone', remaining: 4_301 },
  { stage: 'subscribed', remaining: 4_262 },
  { stage: 'uniquePhone', remaining: 4_138 },
]

const SEGMENT = {
  tier: 'ALL' as const, total: 4_138, target: 3_724, holdout: 414,
  totalLtv: 6_210_400, avgLtv: 1_501, totalRevenue: 11_480_200,
  totalMargin: 6_210_400, marginPct: 54.1, avgOrders: 2.6, avgRecencyDays: 71,
}

const ONE_GROUP: SmsSegmentsResponse = {
  campaign: 'default', salesType: 'retail', ltvBasis: 'margin', grouping: 'single',
  criteria: { ...CRITERIA, maxRecencyDays: 92,
              groups: [{ maxRecencyDays: 92, filters: {} }] },
  funnel: FUNNEL,
  segments: [{ ...SEGMENT, total: 3_103, target: 2_793, holdout: 310 }],
  totals: { customers: 3_103, target: 2_793, holdout: 310 },
  truncated: false,
}

const TWO_GROUPS: SmsSegmentsResponse = {
  campaign: 'default', salesType: 'retail', ltvBasis: 'margin', grouping: 'single',
  criteria: {
    ...CRITERIA, maxRecencyDays: 365,
    groups: [
      { maxRecencyDays: 365, filters: { period_orders_min: 2, period_from: '2026-01-01' } },
      { maxRecencyDays: 92, filters: {} },
    ],
  },
  groups: [
    { group: 1, matched: 2_739, recipients: 2_626, onlyHere: 1_035 },
    { group: 2, matched: 3_237, recipients: 3_103, onlyHere: 1_512 },
  ],
  funnel: FUNNEL,
  segments: [SEGMENT],
  totals: { customers: 4_138, target: 3_724, holdout: 414 },
  truncated: false,
}

const meta = {
  title: 'SMS/SmsAudiencePreview',
  component: SmsAudiencePreview,
  parameters: { layout: 'padded' },
} satisfies Meta<typeof SmsAudiencePreview>

export default meta
type Story = StoryObj<typeof meta>

/** One group: the arm names its window, as it always did. */
export const OneGroup: Story = { args: { data: ONE_GROUP } }

/** Two groups ORed: the arm is "anyone in any of the 2 groups". */
export const TwoGroups: Story = { args: { data: TWO_GROUPS } }

export const Loading: Story = { args: { isLoading: true } }
