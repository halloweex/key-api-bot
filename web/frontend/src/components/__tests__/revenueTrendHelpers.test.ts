import { describe, it, expect } from 'vitest'
import { formatShortCurrency, getComparisonLabel } from '../revenueTrendHelpers'

describe('formatShortCurrency', () => {
  it('formats millions with one decimal + M', () => {
    expect(formatShortCurrency(1_000_000)).toBe('₴1.0M')
    expect(formatShortCurrency(2_500_000)).toBe('₴2.5M')
  })

  it('formats thousands with K (no decimal)', () => {
    expect(formatShortCurrency(1_000)).toBe('₴1K')
    expect(formatShortCurrency(45_320)).toBe('₴45K')
  })

  it('formats raw value below thousand', () => {
    expect(formatShortCurrency(0)).toBe('₴0')
    expect(formatShortCurrency(999)).toBe('₴999')
  })
})

describe('getComparisonLabel', () => {
  const t = (k: string) => k
  it('returns localized "last year" for year_ago', () => {
    expect(getComparisonLabel('year_ago', 'fallback', t)).toBe('chart.lastYear')
  })
  it('returns localized "last month" for month_ago', () => {
    expect(getComparisonLabel('month_ago', 'fallback', t)).toBe('chart.lastMonth')
  })
  it('falls back to base label for previous_period', () => {
    expect(getComparisonLabel('previous_period', 'Yesterday', t)).toBe('Yesterday')
  })
})
