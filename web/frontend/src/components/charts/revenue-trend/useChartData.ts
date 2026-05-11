import { useMemo } from 'react'
import type { RevenueTrendResponse } from '../../../types/api'
import { formatShortCurrency } from './helpers'
import type { ChartDataPoint } from './types'

interface ChartDataResult {
  chartData: ChartDataPoint[]
  hasComparison: boolean
  hasPrevMonthDays: boolean
  hasForecast: boolean
}

export function useChartData(
  data: RevenueTrendResponse | undefined,
  period: string,
): ChartDataResult {
  const forecast = data?.forecast

  return useMemo(() => {
    if (!data?.labels?.length) {
      return { chartData: [], hasComparison: false, hasPrevMonthDays: false, hasForecast: false }
    }

    const hasComp = (data.comparison?.revenue?.length ?? 0) > 0

    // Find top 5 peak indices for labels
    const revenues = data.revenue ?? []
    const revenueWithIndex = revenues.map((rev, idx) => ({ rev, idx }))
    const topPeakIndices = new Set(
      revenueWithIndex
        .filter((item) => item.rev > 0)
        .sort((a, b) => b.rev - a.rev)
        .slice(0, 5)
        .map((item) => item.idx),
    )

    // Get current month for comparison (only relevant for last_28_days)
    // Month is 1-indexed in the date format (01 = January)
    const currentMonth = new Date().getMonth() + 1

    let prevMonthCount = 0

    const processed: ChartDataPoint[] = data.labels.map((label, index) => {
      const revenue = data.revenue?.[index] ?? 0
      const orders = data.orders?.[index] ?? 0
      const prevRevenue = data.comparison?.revenue?.[index] ?? 0
      const prevOrders = data.comparison?.orders?.[index] ?? 0

      const change = revenue - prevRevenue
      const changePercent = prevRevenue > 0 ? (change / prevRevenue) * 100 : 0

      const shortDate = label.length > 6 ? label.slice(0, 6) : label

      const isPeak = topPeakIndices.has(index)

      // Parse month from label (format: "dd.mm" like "21.01" for January 21st)
      let isCurrentMonth = true
      if (period === 'last_28_days') {
        const parts = label.split('.')
        if (parts.length >= 2) {
          const labelMonth = parseInt(parts[1], 10)
          if (labelMonth !== currentMonth) {
            isCurrentMonth = false
            prevMonthCount++
          }
        }
      }

      return {
        date: label,
        shortDate,
        revenue,
        forecastRevenue: 0,
        orders,
        prevRevenue,
        prevOrders,
        change,
        changePercent,
        isPeak,
        peakLabel: isPeak ? formatShortCurrency(revenue) : '',
        isCurrentMonth,
        isForecast: false,
      }
    })

    // Merge or append forecast days if available
    let forecastAppended = false
    if (forecast?.daily_predictions?.length) {
      const labelIndex = new Map<string, number>()
      processed.forEach((p, idx) => labelIndex.set(p.date, idx))

      for (const pred of forecast.daily_predictions) {
        const parts = pred.date.split('-') // "2026-01-30"
        if (parts.length === 3) {
          const label = `${parts[2]}.${parts[1]}` // "30.01"
          const existingIdx = labelIndex.get(label)

          if (existingIdx !== undefined) {
            const actual = processed[existingIdx].revenue
            const predicted = Math.round(pred.predicted_revenue)
            const remaining = Math.max(0, predicted - actual)
            if (actual === 0 || remaining > 0) {
              processed[existingIdx].forecastRevenue = actual === 0 ? predicted : remaining
              processed[existingIdx].isForecast = actual === 0
              processed[existingIdx].fullDayForecast = predicted
            }
          } else if (existingIdx === undefined) {
            processed.push({
              date: label,
              shortDate: label,
              revenue: 0,
              forecastRevenue: Math.round(pred.predicted_revenue),
              orders: 0,
              prevRevenue: 0,
              prevOrders: 0,
              change: 0,
              changePercent: 0,
              isPeak: false,
              peakLabel: '',
              isCurrentMonth: true,
              isForecast: true,
            })
          }
          forecastAppended = true
        }
      }
    }

    return {
      chartData: processed,
      hasComparison: hasComp,
      hasPrevMonthDays: prevMonthCount > 0,
      hasForecast: forecastAppended,
    }
  }, [data, period, forecast])
}
