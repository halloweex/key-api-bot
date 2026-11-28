import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { SmsLiftForest, domainOf, ticksFor, type ForestRow } from '../SmsLiftForest'
import type { SmsComparison } from '../../types/api'

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

function comparison(over: Partial<SmsComparison> = {}): SmsComparison {
  return {
    conversionTarget: 3, conversionHoldout: 2, liftPp: 1, liftRelativePct: 50,
    ci95Pp: [-0.5, 2.5], pValue: 0.2, significant: false,
    incrementalRevenuePerContact: 10, incrementalMarginPerContact: 5,
    incrementalRevenueTotal: 1000, incrementalMarginTotal: 500,
    verdictReady: true, eventsTarget: 30, eventsHoldout: 20, minEvents: 5,
    ...over,
  }
}

function row(label: string, over: Partial<SmsComparison> = {}): ForestRow {
  return { label, comparison: comparison(over), contacts: 1000 }
}

describe('SmsLiftForest', () => {
  it('puts every row on one scale that holds the widest interval', () => {
    // One shared domain is what makes rows comparable; each row used to get its own.
    const rows = [
      row('narrow', { ci95Pp: [-0.2, 0.4], liftPp: 0.1 }),
      row('wide', { ci95Pp: [-6, 9], liftPp: 1.5 }),
    ]
    const [lo, hi] = domainOf(rows)
    expect(lo).toBeLessThanOrEqual(-6)
    expect(hi).toBeGreaterThanOrEqual(9)
  })

  it('pads the domain so the interval ends are not flush with the edge', () => {
    // width 10, padded by 12% of it each side
    const [lo, hi] = domainOf([row('a', { ci95Pp: [-4, 6] })])
    expect(lo).toBeCloseTo(-5.2)
    expect(hi).toBeCloseTo(7.2)
  })

  it('draws a proven interval in the state colour and an unproven one grey', () => {
    const { container } = render(
      <SmsLiftForest
        rows={[
          row('proven', { ci95Pp: [1.2, 4.4], significant: true }),
          row('unproven'),
        ]}
      />,
    )

    // The key uses both swatches, so count the marks inside rows only.
    expect(container.querySelectorAll('.bg-emerald-600').length).toBeGreaterThan(1)
    expect(container.querySelectorAll('.bg-slate-400').length).toBeGreaterThan(1)
  })

  it('labels every row with its lift, which is what makes grey legible', () => {
    render(<SmsLiftForest rows={[row('a', { liftPp: 2.35 }), row('b', { liftPp: -1.2 })]} />)

    expect(screen.getByText('+2.4')).toBeTruthy()
    expect(screen.getByText('-1.2')).toBeTruthy()
  })

  it('always includes zero on the axis even when no interval reaches it', () => {
    // Zero is the "no effect" line; an all-positive campaign must still show it.
    const rows = [row('all positive', { ci95Pp: [2, 6], liftPp: 4 })]
    const domain = domainOf(rows)
    expect(domain[0]).toBeLessThanOrEqual(0)
    expect(domain[1]).toBeGreaterThanOrEqual(6)
    expect(ticksFor(domain)).toContain(0)
    render(<SmsLiftForest rows={rows} />)
    expect(screen.getByText('0')).toBeTruthy()
  })

  it('labels the axis in round steps, all inside the domain', () => {
    expect(ticksFor([-5.2, 7.2])).toEqual([-5, 0, 5])
    expect(ticksFor([-0.7, 1.1])).toEqual([-0.5, 0, 0.5, 1])
  })

  it('reveals the figures on hover rather than printing them on every mark', () => {
    const { container } = render(<SmsLiftForest rows={[row('Tier 2')]} />)

    expect(screen.getByText('sms.forestHint')).toBeTruthy()
    fireEvent.mouseEnter(container.querySelector('.grid')!)
    expect(screen.getByText(/sms\.forestDetail/)).toBeTruthy()
  })

  it('renders nothing when there is nothing to compare', () => {
    const { container } = render(<SmsLiftForest rows={[]} />)
    expect(container.firstChild).toBeNull()
  })
})
