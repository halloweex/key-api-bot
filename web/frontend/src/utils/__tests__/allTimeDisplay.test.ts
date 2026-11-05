import { afterEach, describe, expect, it } from 'vitest'
import i18n from '../../lib/i18n'
import { formatCurrencyShort, formatReportRange } from '../formatters'
import { trafficByMonth, trafficLabel } from '../trafficTrend'
import type { TrafficTrendDay } from '../../types/api'

// Intl puts a no-break space between thousands and before units.
const plain = (s: string) => s.replace(/\s/g, ' ')

afterEach(async () => { await i18n.changeLanguage('en') })

describe('a figure too wide for its tile', () => {
  it('is said in millions from a million up, in the reader\'s language', async () => {
    await i18n.changeLanguage('uk')
    expect(plain(formatCurrencyShort(108_007_791.37))).toBe('₴108 млн')
    expect(plain(formatCurrencyShort(68_106_047))).toBe('₴68,1 млн')
    await i18n.changeLanguage('en')
    expect(formatCurrencyShort(68_106_047)).toBe('₴68.1M')
  })

  it('stays whole below a million, where it is already the shorter', async () => {
    await i18n.changeLanguage('uk')
    expect(plain(formatCurrencyShort(585_704))).toBe('₴585 704')
    expect(plain(formatCurrencyShort(999_999))).toBe('₴999 999')
  })
})

describe('the report\'s dates', () => {
  it('give the start its year when it is not the end\'s', async () => {
    await i18n.changeLanguage('uk')
    expect(formatReportRange('2023-12-02', '2026-11-05')).toBe('02.12.2023 – 05.11.2026')
  })

  it('leave it out inside one year, and name a whole month', async () => {
    await i18n.changeLanguage('uk')
    expect(formatReportRange('2026-11-01', '2026-11-05')).toBe('01.11 – 05.11.2026')
    expect(plain(formatReportRange('2026-10-01', '2026-10-31'))).toBe('ЖОВТЕНЬ 2026 Р.')
  })
})

const day = (date: string, paid: number, organic: number, other: number): TrafficTrendDay => ({
  date,
  paid_orders: 1, paid_revenue: paid,
  organic_orders: 2, organic_revenue: organic,
  other_orders: 3, other_revenue: other,
})

describe('the traffic trend by month', () => {
  it('adds a month\'s days up, oldest month first', () => {
    const months = trafficByMonth([
      day('2024-01-31', 10, 20, 30),
      day('2023-12-02', 1, 2, 3),
      day('2024-01-01', 5, 5, 5),
    ])
    expect(months).toEqual([
      { date: '2023-12', paid_orders: 1, paid_revenue: 1, organic_orders: 2, organic_revenue: 2, other_orders: 3, other_revenue: 3 },
      { date: '2024-01', paid_orders: 2, paid_revenue: 15, organic_orders: 4, organic_revenue: 25, other_orders: 6, other_revenue: 35 },
    ])
  })

  it('leaves the days it was handed as they were', () => {
    const days = [day('2024-01-01', 5, 5, 5), day('2024-01-02', 1, 1, 1)]
    trafficByMonth(days)
    expect(days[0].paid_revenue).toBe(5)
  })

  it('labels a day dd.mm and a month MM.YYYY', () => {
    expect(trafficLabel('2026-02-01')).toBe('01.02')
    expect(trafficLabel('2023-12')).toBe('12.2023')
  })
})
