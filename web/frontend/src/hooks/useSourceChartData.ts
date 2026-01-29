import { useMemo } from 'react'
import { useSalesBySource } from './useApi'
import { SOURCE_COLORS } from '../utils/colors'

// ─── Types ───────────────────────────────────────────────────────────────────

export interface SourceChartDataPoint {
  name: string
  revenue: number
  orders: number
  color: string
  revenuePercent: number
  ordersPercent: number
  label?: string
  [key: string]: string | number | undefined  // Recharts compatibility
}

export interface SourceChartData {
  chartData: SourceChartDataPoint[]
  totalRevenue: number
  totalOrders: number
  isEmpty: boolean
  isLoading: boolean
  error: Error | null
  refetch: () => void
}

// ─── Hook ────────────────────────────────────────────────────────────────────

/**
 * Shared hook for source chart data transformation.
 * Extracts common logic from SalesBySourceChart, OrdersBySourceChart, and RevenueBySourceChart.
 */
export function useSourceChartData(): SourceChartData {
  const { data, isLoading, error, refetch } = useSalesBySource()

  const result = useMemo(() => {
    if (!data?.labels?.length) {
      return {
        chartData: [] as SourceChartDataPoint[],
        totalRevenue: 0,
        totalOrders: 0,
      }
    }

    // Calculate totals first
    const totalRevenue = data.revenue?.reduce((sum, val) => sum + (val ?? 0), 0) ?? 0
    const totalOrders = data.orders?.reduce((sum, val) => sum + (val ?? 0), 0) ?? 0

    const chartData = data.labels.map((label, index) => {
      const revenue = data.revenue?.[index] ?? 0
      const orders = data.orders?.[index] ?? 0
      const color = data.backgroundColor?.[index] ?? SOURCE_COLORS[index % 3] ?? '#2563EB'
      const revenuePercent = totalRevenue > 0 ? (revenue / totalRevenue) * 100 : 0
      const ordersPercent = totalOrders > 0 ? (orders / totalOrders) * 100 : 0

      return {
        name: label,
        revenue,
        orders,
        color,
        revenuePercent,
        ordersPercent,
      }
    })

    return { chartData, totalRevenue, totalOrders }
  }, [data])

  const isEmpty = !isLoading && result.chartData.length === 0

  return {
    ...result,
    isEmpty,
    isLoading,
    error: error as Error | null,
    refetch,
  }
}
