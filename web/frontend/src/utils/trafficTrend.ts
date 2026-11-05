import type { TrafficTrendDay } from '../types/api'

/**
 * The traffic trend's days folded into calendar months, oldest first, each
 * dated `YYYY-MM`.
 *
 * Every figure the trend carries is a count or a sum, so a month is exactly
 * its days added up — no server round trip is needed, unlike the AOV trend,
 * whose ratio has to be taken over the month's sums. A month the answer holds
 * no day of is absent, as a day with no orders is absent from the daily trend.
 * The page asks for this for the whole history, where by day the chart is a
 * thousand bars whose `dd.mm` repeats every year.
 */
export function trafficByMonth(days: TrafficTrendDay[]): TrafficTrendDay[] {
  const months = new Map<string, TrafficTrendDay>()
  for (const day of days) {
    const key = day.date.slice(0, 7)
    const month = months.get(key)
    if (!month) {
      months.set(key, { ...day, date: key })
      continue
    }
    month.paid_orders += day.paid_orders
    month.paid_revenue += day.paid_revenue
    month.organic_orders += day.organic_orders
    month.organic_revenue += day.organic_revenue
    month.other_orders += day.other_orders
    month.other_revenue += day.other_revenue
  }
  return [...months.values()].sort((a, b) => a.date.localeCompare(b.date))
}

/** `2026-02-01` → `01.02`, `2026-02` → `02.2026`: the axis label of a day or a month. */
export function trafficLabel(date: string): string {
  const parts = date.split('-')
  if (parts.length === 3) return `${parts[2]}.${parts[1]}`
  if (parts.length === 2) return `${parts[1]}.${parts[0]}`
  return date
}
