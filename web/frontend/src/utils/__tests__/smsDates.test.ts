import { afterEach, describe, it, expect } from 'vitest'
import i18n from '../../lib/i18n'
import {
  addDays, daysAgo, daysBetween, formatDay, formatDayRange, inclusiveDays, isIsoDate, kyivToday,
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
