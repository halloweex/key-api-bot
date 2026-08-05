import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { SmsSegmentCards } from '../SmsSegmentCards'
import type { SmsSegmentsResponse } from '../../types/api'

// src/lib/i18n.ts reads localStorage while the module is being imported, and
// jsdom here exposes the object without its methods.
vi.hoisted(() => {
  const store = new Map<string, string>()
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => void store.set(k, String(v)),
      removeItem: (k: string) => void store.delete(k),
      clear: () => store.clear(),
    },
  })
})

// Interpolation is echoed into the key so assertions can read the numbers the
// component computed. `initReactI18next` still has to be exported: formatters
// pulls in lib/i18n, which registers it at import time.
vi.mock('react-i18next', () => ({
  initReactI18next: { type: '3rdParty', init: () => {} },
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) =>
      opts ? `${key}(${JSON.stringify(opts)})` : key,
  }),
}))

const data: SmsSegmentsResponse = {
  campaign: 'default',
  salesType: 'retail',
  ltvBasis: 'margin',
  criteria: {
    maxRecencyDays: 270, ltvBasis: 'margin', vipLtv: 5500, coreLtv: 2750,
    coreMinOrders: 2, reactivationMaxRecency: 120, holdoutPct: 10,
  },
  funnel: [
    { stage: 'customers', remaining: 40000 },
    { stage: 'inWindow', remaining: 12000 },
    { stage: 'tiered', remaining: 9000 },
    { stage: 'phone', remaining: 8600 },
    { stage: 'subscribed', remaining: 8500 },
    { stage: 'uniquePhone', remaining: 8400 },
  ],
  segments: [
    { tier: 'VIP', total: 1000, target: 900, holdout: 100, totalLtv: 0,
      avgLtv: 11000, totalRevenue: 0, totalMargin: 0, marginPct: 55,
      avgOrders: 4.8, avgRecencyDays: 61 },
    { tier: 'CORE', total: 3000, target: 2700, holdout: 300, totalLtv: 0,
      avgLtv: 3900, totalRevenue: 0, totalMargin: 0, marginPct: 55,
      avgOrders: 2.3, avgRecencyDays: 98 },
    { tier: 'REACTIVATION', total: 4400, target: 3960, holdout: 440, totalLtv: 0,
      avgLtv: 1300, totalRevenue: 0, totalMargin: 0, marginPct: 55,
      avgOrders: 1, avgRecencyDays: 87 },
  ],
  totals: { customers: 8400, target: 7560, holdout: 840 },
  truncated: false,
}

vi.mock('../../hooks/useApi', () => ({
  useSmsSegments: () => ({ data, isLoading: false, error: null, refetch: vi.fn() }),
  useSmsChannels: () => ({ data: { sms: true, viber: false } }),
  useSendTestSms: () => ({ mutate: vi.fn(), isPending: false }),
}))

vi.mock('../Toast', () => ({ useToast: () => ({ addToast: vi.fn() }) }))

const open = vi.fn()

beforeEach(() => {
  open.mockClear()
  vi.stubGlobal('open', open)
})

/** The querystring the export was opened with. */
function exportedParams(): URLSearchParams {
  return new URLSearchParams(String(open.mock.calls[0][0]).split('?')[1])
}

async function exportWith(...tiers: string[]) {
  render(<SmsSegmentCards />)
  for (const tier of tiers) {
    await userEvent.click(screen.getByRole('button', { name: new RegExp(tier) }))
  }
  await userEvent.click(screen.getByRole('button', { name: 'sms.downloadCsv' }))
}

describe('SmsSegmentCards tier picker', () => {
  it('sends no tier at all when none is picked, meaning every tier', async () => {
    await exportWith()

    expect(exportedParams().has('tier')).toBe(false)
  })

  it('carries the picked tiers as one campaign, not two exports', async () => {
    // A discount suits Core and Reactivation and cannibalises VIP — that is the
    // case the picker exists for.
    await exportWith('sms.tier.CORE', 'sms.tier.REACTIVATION')

    expect(exportedParams().get('tier')).toBe('CORE,REACTIVATION')
  })

  it('drops a tier when it is clicked again', async () => {
    await exportWith('sms.tier.CORE', 'sms.tier.VIP', 'sms.tier.VIP')

    expect(exportedParams().get('tier')).toBe('CORE')
  })

  it('states how many contacts the picked tiers actually come to', async () => {
    render(<SmsSegmentCards />)
    await userEvent.click(screen.getByRole('button', { name: /sms\.tier\.CORE/ }))
    await userEvent.click(
      screen.getByRole('button', { name: /sms\.tier\.REACTIVATION/ }),
    )

    // 2,700 + 3,960 target contacts, the holdout excluded.
    expect(screen.getByText(/6,660/)).toBeTruthy()
  })

  it('shows each tier its own target size on the control', () => {
    render(<SmsSegmentCards />)

    expect(
      screen.getByRole('button', { name: /sms\.tier\.VIP · 900/ }),
    ).toBeTruthy()
  })
})
