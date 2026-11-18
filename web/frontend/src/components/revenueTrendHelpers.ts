import type { CompareType, PeriodLabels } from './revenueTrendTypes'

export const formatShortCurrency = (value: number): string => {
  if (value >= 1000000) {
    return `₴${(value / 1000000).toFixed(1)}M`
  }
  if (value >= 1000) {
    return `₴${(value / 1000).toFixed(0)}K`
  }
  return `₴${value}`
}

/**
 * A range in the badge's units, the currency once: `₴2.9–3.6M`. Millions
 * take one decimal, as the badge's point does, and a second only where one
 * would print both ends the same.
 */
export const formatShortRange = (lo: number, hi: number): string => {
  if (hi >= 1000000) {
    const one = [lo, hi].map((v) => (v / 1000000).toFixed(1))
    const ends = one[0] === one[1] ? [lo, hi].map((v) => (v / 1000000).toFixed(2)) : one
    return `₴${ends[0]}–${ends[1]}M`
  }
  if (hi >= 1000) {
    return `₴${(lo / 1000).toFixed(0)}–${(hi / 1000).toFixed(0)}K`
  }
  return `₴${Math.round(lo)}–${Math.round(hi)}`
}

/** `27.11–29.11.2025` from two ISO dates. */
export const formatDayRange = (start: string, end: string): string => {
  const day = (iso: string) => `${iso.slice(8, 10)}.${iso.slice(5, 7)}`
  return start === end
    ? `${day(start)}.${start.slice(0, 4)}`
    : `${day(start)}–${day(end)}.${end.slice(0, 4)}`
}

export const getPeriodLabels = (t: (key: string) => string): Record<string, PeriodLabels> => ({
  today: { current: t('filter.today'), previous: t('filter.yesterday') },
  yesterday: { current: t('filter.yesterday'), previous: t('chart.dayBefore') },
  week: { current: t('filter.thisWeek'), previous: t('filter.lastWeek') },
  last_week: { current: t('filter.lastWeek'), previous: t('chart.weekBefore') },
  month: { current: t('filter.thisMonth'), previous: t('filter.lastMonth') },
  last_month: { current: t('filter.lastMonth'), previous: t('chart.previousMonth') },
  last_7_days: { current: t('filter.7days'), previous: t('chart.previous7Days') },
  last_28_days: { current: t('filter.28days'), previous: t('chart.previous28Days') },
  // Nothing is drawn beside it (`hasComparison`); the label is never shown.
  all_time: { current: t('filter.allTime'), previous: t('chart.previous') },
  custom: { current: t('chart.selected'), previous: t('chart.previous') },
})

export const getCompareTypeOptions = (
  t: (key: string) => string,
): { value: CompareType; label: string; shortLabel: string }[] => [
  { value: 'year_ago', label: t('chart.yearOverYear'), shortLabel: t('chart.yearAgoShort') },
  { value: 'month_ago', label: t('chart.monthOverMonth'), shortLabel: t('chart.monthAgoShort') },
  { value: 'previous_period', label: t('chart.priorPeriod'), shortLabel: t('chart.previousShort') },
]

export const getComparisonLabel = (
  compareType: CompareType,
  basePeriodLabel: string,
  t: (key: string) => string,
): string => {
  switch (compareType) {
    case 'year_ago':
      return t('chart.lastYear')
    case 'month_ago':
      return t('chart.lastMonth')
    default:
      return basePeriodLabel
  }
}
