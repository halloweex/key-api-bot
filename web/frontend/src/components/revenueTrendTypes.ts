export interface ChartDataPoint {
  date: string
  shortDate: string
  revenue: number
  forecastRevenue: number
  orders: number
  prevRevenue: number
  prevOrders: number
  change: number
  changePercent: number
  isPeak: boolean
  peakLabel: string
  isCurrentMonth: boolean
  isForecast: boolean
  /** Full-day predicted revenue (for today: actual + remaining) */
  fullDayForecast?: number
}

export type CompareType = 'previous_period' | 'year_ago' | 'month_ago'

export interface PeriodLabels {
  current: string
  previous: string
}

// Previous month bar color (lighter/muted version of primary)
export const PREV_MONTH_BAR_COLOR = '#93c5fd' // Light blue (tailwind blue-300)

// Forecast bar color (same blue family, lighter)
export const FORECAST_BAR_COLOR = '#60a5fa' // tailwind blue-400
