import { useState, type ReactNode } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SmsCampaignWizard } from './SmsCampaignWizard'
import { ToastProvider } from './Toast'
import {
  definitionToPreset, emptyDefinition, type AudienceDefinition,
} from '../utils/smsConditions'
import type { SmsSegmentsResponse } from '../types/api'

/**
 * The whole wizard, against a fake API.
 *
 * The other SMS stories show the pieces; this one shows them where they live —
 * the name, the templates, the groups, the campaign-wide level and the sticky
 * audience bar — because a step is judged by how it reads top to bottom, and
 * on a phone by what stays under the thumb.
 *
 * `fetch` is answered from fixtures: the preview returns a share per group it
 * was sent (so the cards and the overlap fill in), and a count of 21+ orders
 * matches nobody, as it does on production. A template's own query answers
 * with what production counted for it on 2026-10-17, so the chips read as
 * they do there.
 */

const BASE: SmsSegmentsResponse = {
  campaign: 'sms-2026-10-17', salesType: 'retail', ltvBasis: 'margin',
  criteria: {
    maxRecencyDays: 3650, ltvBasis: 'margin', vipLtv: 5500, coreLtv: 2750,
    coreMinOrders: 2, reactivationMaxRecency: 120, holdoutPct: 10,
  },
  funnel: [
    { stage: 'customers', remaining: 30_212 },
    { stage: 'inWindow', remaining: 30_212 },
    { stage: 'filtered', remaining: 4_380 },
    { stage: 'tiered', remaining: 4_380 },
    { stage: 'phone', remaining: 4_190 },
    { stage: 'subscribed', remaining: 4_166 },
    { stage: 'reachable', remaining: 4_163 },
    { stage: 'uniquePhone', remaining: 4_101 },
  ],
  segments: [{
    tier: 'ALL', total: 4_101, target: 3_691, holdout: 410, totalLtv: 0, avgLtv: 3_120,
    totalRevenue: 0, totalMargin: 0, marginPct: 54, avgOrders: 2.6, avgRecencyDays: 71,
  }],
  totals: { customers: 4_101, target: 3_691, holdout: 410 },
  truncated: false,
}

/** What production counted for each template's own query (no campaign name,
 *  one group), 2026-10-17, retail. */
function templateCount(p: URLSearchParams): number | null {
  if (p.has('campaign') || p.has('or_groups')) return null
  const has = (k: string) => p.has(k)
  if (p.get('tier') === 'VIP') return 2_041
  if (p.get('period_days') === '30') return 1_185
  if (has('first_order_from')) return 507
  if (p.get('orders_min') === '1' && p.get('orders_max') === '1') return 10_369
  if (p.get('recency_min') === '90') return 2_662
  if (p.get('recency_min') === '240') return 10_728
  if (p.get('orders_min') === '2' && !has('recency_min')) return 6_729
  if (p.get('orders_min') === '1' && p.get('max_recency_days') === '3650') return 17_891
  return null
}

function segments(params: URLSearchParams): SmsSegmentsResponse {
  const counted = templateCount(params)
  if (counted != null) {
    const target = Math.round(counted * 0.9)
    return {
      ...BASE,
      groups: [{ group: 1, matched: counted, recipients: counted, onlyHere: counted }],
      totals: { customers: counted, target, holdout: counted - target },
    }
  }
  const extra = params.has('or_groups') ? JSON.parse(params.get('or_groups')!).length : 0
  const nobody = Number(params.get('orders_min')) > 20
  const shares = [
    { group: 1, matched: 2_800, recipients: 2_612, onlyHere: 1_025 },
    { group: 2, matched: 3_300, recipients: 3_076, onlyHere: 1_489 },
  ]
  const groups = Array.from({ length: extra + 1 }, (_, i) => shares[i] ?? {
    group: i + 1, matched: 900, recipients: 840, onlyHere: 120,
  })
  if (nobody) {
    groups[0] = { group: 1, matched: 0, recipients: 0, onlyHere: 0 }
    return { ...BASE, groups, totals: { customers: 0, target: 0, holdout: 0 }, segments: [] }
  }
  return { ...BASE, groups }
}

const CAMPAIGNS = {
  campaigns: [{
    campaign: 'oct-promo', ltvBasis: 'margin', salesType: 'retail', holdoutPct: 10,
    promocode: null, exportedAt: '2026-10-01T09:00:00Z', sentAt: '2026-10-01T10:00:00Z',
    notes: null, members: 4_064, target: 3_635, holdout: 429, followUpOf: null,
    criteria: {
      grouping: 'single', ltvBasis: 'margin', holdoutPct: 10,
      groups: [
        { maxRecencyDays: 3650,
          filters: { period_orders_min: 2, period_from: '2026-01-01', period_to: '2026-09-28' } },
        { maxRecencyDays: 92, filters: {} },
      ],
    },
  }],
}

const PRESETS = {
  presets: [
    { name: 'RFM tiers', builtin: true, createdBy: null, createdAt: null, updatedAt: null,
      criteria: { grouping: 'rfm', ltvBasis: 'margin', maxRecencyDays: 270, holdoutPct: 10,
                  filters: {} } },
    { name: 'Anua buyers', builtin: false, createdBy: 1, createdAt: null, updatedAt: null,
      criteria: { grouping: 'single', holdoutPct: 20, filters: { brands: ['Anua'] } } },
  ],
}

function json(body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status: 200, headers: { 'Content-Type': 'application/json' },
  })
}

function installFakeApi() {
  const real = window.fetch.bind(window)
  window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(typeof input === 'string' ? input : input.toString(), window.location.origin)
    const path = url.pathname
    if (!path.startsWith('/api/')) return real(input, init)
    if (path === '/api/customers/sms-segments') return json(segments(url.searchParams))
    if (path === '/api/customers/sms-campaigns') return json(CAMPAIGNS)
    if (path === '/api/customers/sms-audience-presets') return json(PRESETS)
    if (path === '/api/customers/sms/channels') {
      return json({ sms: true, viber: false, smsSender: 'KoreanStory', viberSender: null,
                    pricePerPart: 1.28 })
    }
    if (path === '/api/brands') return json([{ name: 'Anua' }, { name: 'COSRX' }, { name: 'Medicube' }])
    if (path === '/api/categories') return json([{ id: 1, name: 'Догляд за обличчям' }])
    return json({})
  }
}

/** A draft in session storage is how the wizard opens on an audience. */
function seedDraft(audience: AudienceDefinition | null) {
  try {
    sessionStorage.clear()
    if (audience) {
      sessionStorage.setItem('sms.campaignDraft.v2', JSON.stringify({
        audience: definitionToPreset(audience), campaign: '', promocode: '', text: '',
        presetName: '', followUp: false, followUpOf: '',
      }))
    }
  } catch { /* storage blocked: the wizard opens empty */ }
}

function Harness({ audience, children }: { audience: AudienceDefinition | null; children: ReactNode }) {
  // Once per story: seeded before the wizard first reads the draft.
  const [client] = useState(() => {
    installFakeApi()
    seedDraft(audience)
    return new QueryClient({ defaultOptions: { queries: { retry: false } } })
  })
  return (
    <QueryClientProvider client={client}>
      <ToastProvider>{children}</ToastProvider>
    </QueryClientProvider>
  )
}

const OWNER: AudienceDefinition = {
  ...emptyDefinition(),
  groups: [
    { id: 'a', conditions: [{ id: 'a1', type: 'orders', op: 'atLeast', count: 2, countMax: null,
      period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' } }] },
    { id: 'b', conditions: [{ id: 'b1', type: 'orders', op: 'atLeast', count: 1, countMax: null,
      period: { kind: 'range', from: '2026-07-16', to: '2026-10-14' } }] },
  ],
}

const meta = {
  title: 'SMS/SmsCampaignWizard',
  component: SmsCampaignWizard,
  parameters: { layout: 'padded' },
  args: { onClose: () => {} },
} satisfies Meta<typeof SmsCampaignWizard>

export default meta
type Story = StoryObj<typeof meta>

/** How it opens: a suggested name, the templates, one empty group — everybody. */
export const Start: Story = {
  render: (args) => <Harness audience={null}><SmsCampaignWizard {...args} /></Harness>,
}

/** The owner's campaign of 2026-10-17, as the new step says it. */
export const OwnersAudience: Story = {
  render: (args) => <Harness audience={OWNER}><SmsCampaignWizard {...args} /></Harness>,
}

/** The same on a phone: horizontal templates, icon actions, the bar under the thumb. */
export const OwnersAudienceOnAPhone: Story = {
  ...OwnersAudience,
  globals: { viewport: { value: 'mobile2', isRotated: false } },
}

/**
 * Two conditions and nobody: the card asks what the group matches without
 * each one, finds that without «92+ orders» 2,800 would, and that 92+ orders
 * alone matches nobody — so it names that condition.
 */
export const NinetyTwoOrdersBesideAnother: Story = {
  render: (args) => (
    <Harness audience={{
      ...emptyDefinition(),
      groups: [{ id: 'a', conditions: [
        { id: 'a1', type: 'orders', op: 'atLeast', count: 92, countMax: null, period: { kind: 'all' } },
        { id: 'a2', type: 'lastOrder', when: { kind: 'within', days: 365 } },
      ] }],
    }}>
      <SmsCampaignWizard {...args} />
    </Harness>
  ),
}

/** The same on a phone. */
export const NinetyTwoOrdersBesideAnotherOnAPhone: Story = {
  ...NinetyTwoOrdersBesideAnother,
  globals: { viewport: { value: 'mobile2', isRotated: false } },
}

/** The slip of 2026-10-17: 92 typed as an order count. */
export const NinetyTwoOrders: Story = {
  render: (args) => (
    <Harness audience={{
      ...emptyDefinition(),
      groups: [{ id: 'a', conditions: [{ id: 'a1', type: 'orders', op: 'atLeast', count: 92,
        countMax: null, period: { kind: 'all' } }] }],
    }}>
      <SmsCampaignWizard {...args} />
    </Harness>
  ),
}
