import { describe, it, expect, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import type { SmsCampaignResultsResponse, SmsComparison, SmsGroupStats } from '../../types/api'

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

vi.mock('react-i18next', () => ({
  initReactI18next: { type: '3rdParty', init: () => {} },
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) =>
      opts ? `${key}(${JSON.stringify(opts)})` : key,
  }),
}))

const results = vi.hoisted(() => ({ current: null as SmsCampaignResultsResponse | null }))

vi.mock('../../hooks/useApi', () => ({
  useSmsCampaigns: () => ({
    data: { campaigns: [{ campaign: 'aug-promo', sentAt: '2026-08-05T15:03:00Z' }] },
  }),
  useSmsCampaignResults: () => ({
    data: results.current, isLoading: false, error: null,
  }),
}))

import { SmsCampaignResults } from '../SmsCampaignResults'

function stats(over: Partial<SmsGroupStats> = {}): SmsGroupStats {
  return {
    contacts: 1000, converted: 40, orders: 45, revenue: 90000, margin: 30000,
    promoOrders: 0, delivered: 990, undelivered: 10, notSent: 0, ...over,
  }
}

function comparison(over: Partial<SmsComparison> = {}): SmsComparison {
  return {
    conversionTarget: 4, conversionHoldout: 3, liftPp: 1,
    liftRelativePct: 33, ci95Pp: [-0.6, 2.6], pValue: 0.21, significant: false,
    verdictReady: true, eventsTarget: 40, eventsHoldout: 4, minEvents: 5,
    incrementalRevenuePerContact: 12, incrementalMarginPerContact: 4,
    incrementalRevenueTotal: 12000, incrementalMarginTotal: 4000, ...over,
  }
}

function response(over: Partial<SmsCampaignResultsResponse> = {}): SmsCampaignResultsResponse {
  return {
    campaign: 'aug-promo', sentAt: '2026-08-05T15:03:00Z', windowDays: 30,
    ltvBasis: 'margin', holdoutPct: 10, promocode: null, costTotal: null,
    overall: { target: stats(), holdout: stats({ contacts: 120, converted: 4 }), comparison: comparison() },
    segments: [
      { tier: 'VIP', target: stats(), holdout: stats({ contacts: 40, converted: 2 }), comparison: comparison() },
      { tier: 'CORE', target: stats(), holdout: stats({ contacts: 40, converted: 1 }), comparison: comparison() },
    ],
    ...over,
  }
}

function renderWith(data: SmsCampaignResultsResponse) {
  results.current = data
  return render(<SmsCampaignResults />)
}

describe('SmsCampaignResults', () => {
  it('puts every arm on one row, totals first', () => {
    renderWith(response())

    const rows = screen.getAllByRole('row')
    // header + overall + two tiers
    expect(rows).toHaveLength(4)
    expect(within(rows[1]).getByText('sms.overall')).toBeTruthy()
    expect(within(rows[2]).getByText('sms.tier.VIP')).toBeTruthy()
  })

  it('prints each rate over the counts it came from', () => {
    renderWith(response())

    const overall = screen.getAllByRole('row')[1]
    expect(within(overall).getByText('4.0%')).toBeTruthy()   // 40 / 1000 messaged
    expect(within(overall).getByText('3.3%')).toBeTruthy()   // 4 / 120 control
    expect(within(overall).getByText('40 / 1,000')).toBeTruthy()
  })

  it('says the campaign is unmeasured when no interval clears zero', () => {
    renderWith(response())
    expect(screen.getByText('sms.insufficientTitle')).toBeTruthy()
  })

  it('drops that notice as soon as one arm shows an effect', () => {
    renderWith(response({
      overall: {
        target: stats(),
        holdout: stats({ contacts: 120, converted: 4 }),
        comparison: comparison({ ci95Pp: [0.4, 3.1], pValue: 0.01, significant: true }),
      },
    }))

    expect(screen.queryByText('sms.insufficientTitle')).toBeNull()
  })

  it('marks a proven lift and leaves an unproven one plain', () => {
    const { container } = renderWith(response({
      overall: {
        target: stats(),
        holdout: stats({ contacts: 120, converted: 4 }),
        comparison: comparison({ ci95Pp: [0.4, 3.1], significant: true }),
      },
    }))

    const overall = screen.getAllByRole('row')[1]
    expect(within(overall).getByText('+1.0').className).toContain('emerald')
    // The tiers below are still unproven, so nothing there may read as a win.
    expect(container.querySelectorAll('td .text-emerald-700')).toHaveLength(1)
  })

  it('says so in the row when a tier has no control to compare against', () => {
    renderWith(response({
      segments: [{ tier: 'VIP', target: stats(), holdout: stats({ contacts: 0, converted: 0 }), comparison: null }],
    }))

    expect(screen.getByText('sms.noControlInTier')).toBeTruthy()
  })

  it('shows the totals with the per-contact figure they scale from', () => {
    renderWith(response())

    const overall = screen.getAllByRole('row')[1]
    expect(within(overall).getByText('₴12,000')).toBeTruthy()
    expect(within(overall).getByText('12.00 sms.perContact')).toBeTruthy()
    expect(within(overall).getByText('₴4,000')).toBeTruthy()
  })

  it('says "too early" rather than "no effect" while the control is thin', () => {
    renderWith(response({
      overall: {
        target: stats(),
        holdout: stats({ contacts: 120, converted: 1 }),
        comparison: comparison({ verdictReady: false, eventsHoldout: 1 }),
      },
    }))

    // The weaker sentence, and the count that explains it.
    expect(screen.getByText(/sms\.verdictTooEarly.*"n":1/)).toBeTruthy()
    expect(screen.queryByText('sms.verdictInconclusive')).toBeNull()
  })

  it('picks the banner that matches why nothing is proven', () => {
    const thin = { verdictReady: false, eventsHoldout: 1 }
    renderWith(response({
      overall: {
        target: stats(), holdout: stats({ contacts: 120, converted: 1 }),
        comparison: comparison(thin),
      },
      segments: [{
        tier: 'VIP', target: stats(), holdout: stats({ contacts: 40, converted: 0 }),
        comparison: comparison({ verdictReady: false, eventsHoldout: 0 }),
      }],
    }))
    expect(screen.getByText('sms.tooEarlyTitle')).toBeTruthy()
    expect(screen.queryByText('sms.insufficientTitle')).toBeNull()
  })

  it('falls back to the wider-interval wording once the arms have bought', () => {
    renderWith(response())   // verdictReady, nothing significant
    expect(screen.getByText('sms.insufficientTitle')).toBeTruthy()
    expect(screen.queryByText('sms.tooEarlyTitle')).toBeNull()
  })

  it('never paints a lift green while the verdict is withheld', () => {
    const { container } = renderWith(response({
      overall: {
        target: stats(),
        holdout: stats({ contacts: 120, converted: 0 }),
        // An interval clear of zero, but on an arm that has not bought.
        comparison: comparison({
          ci95Pp: [0.4, 3.1], significant: false,
          verdictReady: false, eventsHoldout: 0,
        }),
      },
    }))

    expect(container.querySelectorAll('td .text-emerald-700')).toHaveLength(0)
  })

  it('marks the money as an estimate while the verdict is withheld', () => {
    renderWith(response({
      overall: {
        target: stats(),
        holdout: stats({ contacts: 120, converted: 1 }),
        comparison: comparison({ verdictReady: false, eventsHoldout: 1 }),
      },
    }))

    const overall = screen.getAllByRole('row')[1]
    // Still printed — watching it move is the point — but not as a result.
    expect(within(overall).getByText('₴12,000').className).toContain('slate-400')
    expect(within(overall).getAllByText('sms.moneyEstimate')).toHaveLength(2)
    expect(within(overall).queryByText('12.00 sms.perContact')).toBeNull()
  })

  it('gives the money its full weight once a verdict is offered', () => {
    renderWith(response())

    const overall = screen.getAllByRole('row')[1]
    expect(within(overall).getByText('₴12,000').className).toContain('slate-800')
    expect(within(overall).getByText('12.00 sms.perContact')).toBeTruthy()
  })

  it('states whether the campaign covered what it cost', () => {
    renderWith(response({ costTotal: 5000 }))
    // 4 000 added margin against 5 000 spent — it did not.
    expect(screen.getByText('\u20B4-1,000')).toBeTruthy()
    expect(screen.getByText('sms.payback')).toBeTruthy()
  })

  it('says nothing about payback when no cost was recorded', () => {
    renderWith(response())          // costTotal: null
    expect(screen.queryByText('sms.payback')).toBeNull()
  })

  it('marks the payback as an estimate while the verdict is withheld', () => {
    renderWith(response({
      costTotal: 1000,
      overall: {
        target: stats(), holdout: stats({ contacts: 120, converted: 1 }),
        comparison: comparison({ verdictReady: false, eventsHoldout: 1 }),
      },
    }))
    expect(screen.getAllByText('sms.moneyEstimate').length).toBeGreaterThan(0)
  })

  it('draws no table at all until the results have loaded', () => {
    results.current = null
    render(<SmsCampaignResults />)
    expect(screen.queryByRole('table')).toBeNull()
  })
})
