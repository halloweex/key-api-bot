import type { CompareType, PeriodLabels } from './types'

export const formatShortCurrency = (value: number): string => {
  if (value >= 1000000) {
    return `₴${(value / 1000000).toFixed(1)}M`
  }
  if (value >= 1000) {
    return `₴${(value / 1000).toFixed(0)}K`
  }
  return `₴${value}`
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
