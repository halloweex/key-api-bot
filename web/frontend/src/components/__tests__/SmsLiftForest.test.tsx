import { describe, it, expect, vi } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import { SmsLiftForest, type ForestRow } from '../SmsLiftForest'
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
    ...over,
  }
}

function row(label: string, over: Partial<SmsComparison> = {}): ForestRow {
  return { label, comparison: comparison(over), contacts: 1000 }
}

/** Left offsets of the zero rules, one per row. */
function zeroLines(container: HTMLElement): string[] {
  return Array.from(container.querySelectorAll<HTMLElement>('.bg-slate-300'))
    .map((el) => el.style.left)
}

describe('SmsLiftForest', () => {
  it('puts every row on the same scale — the reason it exists', () => {
    const { container } = render(
      <SmsLiftForest
        rows={[
          row('narrow', { ci95Pp: [-0.2, 0.4], liftPp: 0.1 }),
          row('wide', { ci95Pp: [-6, 9], liftPp: 1.5 }),
        ]}
      />,
    )

    const lines = zeroLines(container)
    expect(lines).toHaveLength(2)
    expect(new Set(lines).size).toBe(1)
  })

  it('keeps every mark inside the plot, so nothing overflows', () => {
    const { container } = render(
      <SmsLiftForest rows={[row('a', { ci95Pp: [-4, 6], liftPp: 1 }), row('b')]} />,
    )

    const offsets = Array.from(container.querySelectorAll<HTMLElement>('[style*="left"]'))
      .map((el) => parseFloat(el.style.left))
    expect(offsets.every((x) => x >= 0 && x <= 100)).toBe(true)
    expect(offsets.some(Number.isNaN)).toBe(false)
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
    const { container } = render(
      <SmsLiftForest rows={[row('all positive', { ci95Pp: [2, 6], liftPp: 4 })]} />,
    )

    expect(zeroLines(container)).toHaveLength(1)
    expect(screen.getByText('0')).toBeTruthy()
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
