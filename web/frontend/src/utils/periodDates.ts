import type { Period } from '../types/filters'
import { addDays, isIsoDate, kyivToday } from './smsDates'

/**
 * The first day the store holds an order, where `all_time` starts:
 * `core/filters.HISTORY_START`, which says how it was measured.
 * `tests/unit/test_period_all_time.py` holds the two equal.
 */
export const HISTORY_START = '2023-12-02'

/**
 * Whether a period has anything before it to be compared with —
 * `core/filters.has_comparison`. The whole history does not: its previous
 * period is all before the first order, and a year back is mostly the same
 * orders. Blocks that set a period beside another hide that half for it.
 */
export function hasComparison(period: Period): boolean {
  return period !== 'all_time'
}

/**
 * The days a period covers, as the server counts them.
 *
 * A transliteration of `core/filters.parse_period`, in Kyiv dates: a week
 * starts on Monday, "this week" and "this month" run to today, the rolling
 * periods include today. The server stays the authority — every answer that
 * carries its own dates is drawn from those — and this exists for the one
 * place nothing has answered yet: the header line that says the chosen
 * period in words.
 */
export function periodDates(
  period: Period,
  startDate: string | null,
  endDate: string | null,
  today: string = kyivToday(),
): { start: string; end: string } | null {
  const [y, m] = today.split('-').map(Number)
  const firstOfMonth = `${today.slice(0, 7)}-01`
  // 0 is Monday, as Python's weekday().
  const weekday = (new Date(Date.UTC(y, m - 1, Number(today.slice(8, 10)))).getUTCDay() + 6) % 7
  const monday = addDays(today, -weekday)

  switch (period) {
    case 'today':
      return { start: today, end: today }
    case 'yesterday': {
      const day = addDays(today, -1)
      return { start: day, end: day }
    }
    case 'week':
      return { start: monday, end: today }
    case 'last_week':
      return { start: addDays(monday, -7), end: addDays(monday, -1) }
    case 'month':
      return { start: firstOfMonth, end: today }
    case 'last_month': {
      const end = addDays(firstOfMonth, -1)
      return { start: `${end.slice(0, 7)}-01`, end }
    }
    case 'last_7_days':
      return { start: addDays(today, -6), end: today }
    case 'last_28_days':
      return { start: addDays(today, -27), end: today }
    case 'all_time':
      return { start: HISTORY_START, end: today }
    case 'custom':
      return isIsoDate(startDate) && isIsoDate(endDate) ? { start: startDate, end: endDate } : null
  }
}
