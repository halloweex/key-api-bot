import { CHART_THEME } from '../config'
import { FORECAST_BAR_COLOR, PREV_MONTH_BAR_COLOR } from './types'

export function RevenueTrendGradients() {
  return (
    <defs>
      <linearGradient id="currentPeriodGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="5%" stopColor={CHART_THEME.primary} stopOpacity={0.3} />
        <stop offset="95%" stopColor={CHART_THEME.primary} stopOpacity={0.02} />
      </linearGradient>
      <linearGradient id="currentBarGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={CHART_THEME.primary} stopOpacity={1} />
        <stop offset="100%" stopColor={CHART_THEME.primary} stopOpacity={0.85} />
      </linearGradient>
      <linearGradient id="prevBarGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={CHART_THEME.muted} stopOpacity={0.5} />
        <stop offset="100%" stopColor={CHART_THEME.muted} stopOpacity={0.25} />
      </linearGradient>
      <linearGradient id="prevMonthBarGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={PREV_MONTH_BAR_COLOR} stopOpacity={0.9} />
        <stop offset="100%" stopColor={PREV_MONTH_BAR_COLOR} stopOpacity={0.7} />
      </linearGradient>
      <linearGradient id="forecastBarGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={FORECAST_BAR_COLOR} stopOpacity={0.45} />
        <stop offset="100%" stopColor={FORECAST_BAR_COLOR} stopOpacity={0.25} />
      </linearGradient>
    </defs>
  )
}
