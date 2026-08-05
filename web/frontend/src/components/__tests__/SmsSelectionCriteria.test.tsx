import { describe, it, expect, vi } from 'vitest'
import { render, fireEvent } from '@testing-library/react'
import { SmsSelectionCriteria } from '../SmsSelectionCriteria'
import type { SmsSegmentsResponse } from '../../types/api'

// src/lib/i18n.ts reads localStorage while the module is being imported, and
// jsdom here exposes the object without its methods. vi.hoisted runs before
// the import graph is evaluated, which a plain assignment would not.
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

// Interpolation is kept in the key so assertions read the numbers the component
// chose rather than the copy around them. `initReactI18next` still has to be
// exported: formatters pulls in lib/i18n, which registers it at import time.
vi.mock('react-i18next', () => ({
  initReactI18next: { type: '3rdParty', init: () => {} },
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) =>
      opts ? `${key}(${JSON.stringify(opts)})` : key,
  }),
}))

function response(over: Partial<SmsSegmentsResponse> = {}): SmsSegmentsResponse {
  return {
    campaign: 'aug-promo',
    salesType: 'retail',
    ltvBasis: 'margin',
    criteria: {
      maxRecencyDays: 270,
      ltvBasis: 'margin',
      vipLtv: 5500,
      coreLtv: 2750,
      coreMinOrders: 2,
      reactivationMaxRecency: 120,
      holdoutPct: 10,
    },
    funnel: [
      { stage: 'customers', remaining: 40000 },
      { stage: 'inWindow', remaining: 12000 },
      { stage: 'tiered', remaining: 9000 },
      { stage: 'phone', remaining: 8600 },
      { stage: 'subscribed', remaining: 8500 },
      { stage: 'uniquePhone', remaining: 8400 },
    ],
    segments: [],
    totals: { customers: 8400, target: 7560, holdout: 840 },
    truncated: false,
    ...over,
  }
}

describe('SmsSelectionCriteria', () => {
  it('leads with the drop from base to sendable before anything is expanded', () => {
    const { getByText, queryByText } = render(<SmsSelectionCriteria data={response()} />)

    expect(
      getByText(/sms\.criteriaSummary.*"base":"40,000".*"sendable":"8,400"/),
    ).toBeTruthy()
    expect(queryByText('sms.funnel.phone')).toBeNull()
  })

  it('reports what each rule removed, not just what survived it', () => {
    const { getByRole, getByText } = render(<SmsSelectionCriteria data={response()} />)
    fireEvent.click(getByRole('button'))

    // inWindow: 40,000 → 12,000
    expect(getByText('−28,000')).toBeTruthy()
    // phone: 9,000 → 8,600
    expect(getByText('−400')).toBeTruthy()
  })

  it('leaves the first stage without a drop — nothing precedes it', () => {
    const { getByRole, container } = render(<SmsSelectionCriteria data={response()} />)
    fireEvent.click(getByRole('button'))

    const firstRow = container.querySelectorAll('tbody tr')[0]
    expect(firstRow.textContent).toContain('sms.funnel.customers')
    expect(firstRow.textContent).not.toContain('−')
  })

  it('states the thresholds actually in force, not the defaults', () => {
    const data = response()
    data.criteria = { ...data.criteria, vipLtv: 9000, coreMinOrders: 3, coreLtv: 4000 }
    const { getByRole, getByText } = render(<SmsSelectionCriteria data={data} />)
    fireEvent.click(getByRole('button'))

    expect(getByText(/sms\.ruleLtvAtLeast.*₴9,000/)).toBeTruthy()
    expect(getByText(/sms\.ruleCoreValue.*"orders":3.*₴4,000/)).toBeTruthy()
  })

  it('survives a stage that removed everyone', () => {
    const { getByRole, getByText } = render(
      <SmsSelectionCriteria
        data={response({
          funnel: [
            { stage: 'customers', remaining: 12 },
            { stage: 'inWindow', remaining: 0 },
            { stage: 'tiered', remaining: 0 },
            { stage: 'phone', remaining: 0 },
            { stage: 'subscribed', remaining: 0 },
            { stage: 'uniquePhone', remaining: 0 },
          ],
        })}
      />,
    )
    fireEvent.click(getByRole('button'))

    expect(getByText('−12')).toBeTruthy()
  })
})
