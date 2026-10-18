import { useState, type ReactNode } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SmsCampaignResults } from './SmsCampaignResults'
import type {
  SmsCampaignResultsResponse, SmsComparison, SmsGroupStats, SmsResultSegment,
} from '../types/api'

/**
 * A campaign read against its control: the answer in a sentence, then one row
 * per segment — the arms the campaign is measured in.
 *
 * A campaign measured as one arm has one row, named after the arm («Вся
 * аудитория»); its totals ARE that arm, and printing «Вся кампания» above an
 * identical row read as two results. With levels (VIP / Core / Reactivation)
 * the whole campaign leads and each level follows. A segment is never an
 * audience group — «Группа 1/2» are the OR-ed parts of the wizard's audience.
 *
 * `fetch` is answered from fixtures. "One arm" is sms-2026-10-17 as the owner
 * read it on 2026-10-18.
 */

const stats = (contacts: number, converted: number, revenue: number): SmsGroupStats => ({
  contacts, converted, orders: converted, revenue, margin: Math.round(revenue * 0.46),
  promoOrders: 0, delivered: Math.round(contacts * 0.96), undelivered: 0, notSent: 0,
})

const comparison = (over: Partial<SmsComparison>): SmsComparison => ({
  conversionTarget: 1.1, conversionHoldout: 0, liftPp: 1.1, liftRelativePct: null,
  ci95Pp: [-0.2, 2.4], pValue: 0.12, significant: false, verdictReady: false,
  eventsTarget: 40, eventsHoldout: 0, minEvents: 5,
  incrementalRevenuePerContact: 34.55, incrementalMarginPerContact: 16.03,
  incrementalRevenueTotal: 125_605, incrementalMarginTotal: 58_262, ...over,
})

const ONE_ARM: SmsCampaignResultsResponse = {
  campaign: 'sms-2026-10-17', sentAt: '2026-10-17T09:05:00Z', windowDays: 30,
  ltvBasis: 'margin', holdoutPct: 10, promocode: null, costTotal: 4_653,
  overall: { target: stats(3635, 40, 125_605), holdout: stats(429, 0, 0), comparison: comparison({}) },
  segments: [{
    tier: 'ALL', target: stats(3635, 40, 125_605), holdout: stats(429, 0, 0),
    comparison: comparison({}),
  }],
}

const level = (tier: SmsResultSegment['tier'], n: number, bought: number, held: number,
               heldBought: number, lift: number): SmsResultSegment => ({
  tier, target: stats(n, bought, bought * 1900), holdout: stats(held, heldBought, heldBought * 1900),
  comparison: comparison({
    liftPp: lift, ci95Pp: [lift - 2.1, lift + 2.1], verdictReady: true, eventsHoldout: heldBought,
  }),
})

const LEVELS: SmsCampaignResultsResponse = {
  ...ONE_ARM,
  campaign: 'aug-summer-sale',
  overall: {
    target: stats(4199, 210, 410_000), holdout: stats(467, 18, 33_000),
    comparison: comparison({ liftPp: 1.15, ci95Pp: [-0.9, 3.2], verdictReady: true, eventsHoldout: 18 }),
  },
  segments: [
    level('VIP', 820, 92, 91, 8, 2.4),
    level('CORE', 1900, 86, 211, 7, 1.2),
    level('REACTIVATION', 1479, 32, 165, 3, 0.3),
  ],
}

function json(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200, headers: { 'Content-Type': 'application/json' },
  })
}

function Harness({ results, children }: { results: SmsCampaignResultsResponse; children: ReactNode }) {
  const [client] = useState(() => {
    const real = window.fetch.bind(window)
    window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(typeof input === 'string' ? input : input.toString(), window.location.origin)
      if (url.pathname === '/api/customers/sms-campaigns') {
        return json({ campaigns: [{
          campaign: results.campaign, sentAt: results.sentAt, ltvBasis: 'margin',
          salesType: 'retail', holdoutPct: 10, promocode: null, exportedAt: results.sentAt,
          notes: null, members: 4064, target: 3635, holdout: 429, followUpOf: null,
        }] })
      }
      if (url.pathname.endsWith('/results')) return json(results)
      return real(input, init)
    }
    return new QueryClient({ defaultOptions: { queries: { retry: false } } })
  })
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}

const meta = {
  title: 'SMS/SmsCampaignResults',
  component: SmsCampaignResults,
} satisfies Meta<typeof SmsCampaignResults>

export default meta
type Story = StoryObj<typeof meta>

/** One arm: one row, «Вся аудитория» — not «Вся кампания» above a copy of it. */
export const OneArm: Story = {
  render: () => <Harness results={ONE_ARM}><SmsCampaignResults /></Harness>,
}

/** Three levels: the whole campaign first, then each segment. */
export const ThreeLevels: Story = {
  render: () => <Harness results={LEVELS}><SmsCampaignResults /></Harness>,
}
