import { afterEach, describe, it, expect } from 'vitest'
import i18n from '../../lib/i18n'
import {
  addDays, daysAgo, daysAgoRange, daysBetween, formatDay, formatDayRange, inclusiveDays,
  isIsoDate, kyivToday, lastDays, rangeEnds,
} from '../smsDates'

afterEach(() => { void i18n.changeLanguage('en') })

const plain = (s: string) => s.replace(/ /g, ' ')

describe('days on the Kyiv calendar', () => {
  it('takes today in Kyiv, not in the browser', () => {
    // 22:30 UTC on 16 October is 01:30 on the 17th in Kyiv (UTC+3).
    expect(kyivToday(new Date('2026-10-16T22:30:00Z'))).toBe('2026-10-17')
    expect(kyivToday(new Date('2026-10-16T20:00:00Z'))).toBe('2026-10-16')
  })

  it('counts whole days across the night the clocks change', () => {
    // Kyiv leaves summer time on 25 October 2026.
    expect(daysBetween('2026-10-24', '2026-10-26')).toBe(2)
    expect(addDays('2026-10-24', 2)).toBe('2026-10-26')
    expect(addDays('2026-03-01', -1)).toBe('2026-02-28')
  })

  it('says a range in days with both ends included', () => {
    expect(inclusiveDays('2026-01-01', '2026-10-14')).toBe(287)
    expect(daysAgo('2026-10-20', '2026-10-17')).toBe(0)
    expect(daysAgo('2026-07-19', '2026-10-17')).toBe(90)
  })

  it('knows a real date from one that only looks like it', () => {
    expect(isIsoDate('2026-02-28')).toBe(true)
    expect(isIsoDate('2026-02-30')).toBe(false)
    expect(isIsoDate('14.10.2026')).toBe(false)
    // What a native date field holds while its year is still being typed.
    expect(isIsoDate('0002-10-14')).toBe(false)
    expect(isIsoDate('')).toBe(false)
  })
})

describe('dates said in words', () => {
  it('in Russian and Ukrainian: day, month, year, without the dots and "г."', async () => {
    await i18n.changeLanguage('ru')
    expect(plain(formatDay('2026-10-14'))).toBe('14 окт 2026')
    expect(plain(formatDayRange('2026-01-01', '2026-10-14'))).toBe('1 янв – 14 окт 2026')
    await i18n.changeLanguage('uk')
    expect(plain(formatDay('2026-10-14'))).toMatch(/^14 жовт 2026$/)
  })

  it('in English, the way English writes it', () => {
    expect(formatDay('2026-10-14')).toBe('Oct 14, 2026')
    expect(formatDayRange('2025-12-01', '2026-01-31')).toBe('Dec 1, 2025 – Jan 31, 2026')
  })
})

// ─── "The last N days" says what the server counts ─────────────────────────
//
// The server's rule for every relative time is `today − order_date <= N` in
// Kyiv dates (`SmsAudienceFilters.period_count`, `recency`, "bought within N
// days"). `tests/unit/test_sms_last_n_days_edge.py` runs that predicate on
// DuckDB for the same day and N; the two tests state the same literal dates,
// so if either side moves an edge, one of them fails.

/** The server's predicate, as TypeScript. */
const serverCounts = (orderDate: string, today: string, n: number) =>
  daysBetween(orderDate, today) <= n

describe('the last N days', () => {
  const TODAY = '2026-10-18'

  it('is today − N to today, both included: N + 1 calendar days', () => {
    expect(lastDays(91, TODAY)).toEqual({ from: '2026-07-19', to: '2026-10-18' })
    expect(inclusiveDays('2026-07-19', '2026-10-18')).toBe(92)
  })

  it.each([1, 30, 90, 91, 92, 180, 365, 3650])(
    'starts on the first day the server counts, N = %i', (n) => {
      const { from, to } = lastDays(n, TODAY)
      expect(serverCounts(from, TODAY, n)).toBe(true)
      expect(serverCounts(addDays(from, -1), TODAY, n)).toBe(false)
      expect(serverCounts(to, TODAY, n)).toBe(true)
    },
  )

  it('says so in full words, the year once', async () => {
    await i18n.changeLanguage('ru')
    const { from, to } = lastDays(91, TODAY)
    expect(plain(i18n.t('periodControl.resolved', rangeEnds(from, to)))).toBe(
      '= с 19 июля по 18 октября 2026, включительно',
    )
    await i18n.changeLanguage('uk')
    expect(plain(i18n.t('periodControl.resolved', rangeEnds(from, to)))).toBe(
      '= з 19 липня по 18 жовтня 2026, включно',
    )
    await i18n.changeLanguage('en')
    expect(plain(i18n.t('periodControl.resolved', rangeEnds(from, to)))).toBe(
      '= July 19 to October 18, 2026, both included',
    )
  })

  it('names the year at both ends when they differ', async () => {
    await i18n.changeLanguage('ru')
    const { from, to } = lastDays(365, TODAY)
    expect(plain(i18n.t('periodControl.resolved', rangeEnds(from, to)))).toBe(
      '= с 18 октября 2025 по 18 октября 2026, включительно',
    )
  })

  it('says "N or more days ago" and "between N and M days ago" by the same rule', () => {
    // recency >= N  ⇔  order date <= today − N; recency <= M  ⇔  date >= today − M.
    expect(daysAgoRange(60, null, TODAY)).toEqual({ from: undefined, to: '2026-08-19' })
    expect(daysAgoRange(90, 180, TODAY)).toEqual({ from: '2026-04-21', to: '2026-07-20' })
    expect(daysBetween('2026-07-20', TODAY)).toBe(90)
    expect(daysBetween('2026-04-21', TODAY)).toBe(180)
  })
})
