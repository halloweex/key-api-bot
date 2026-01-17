import { useMemo, memo } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import {
  CHART_THEME,
  CHART_DIMENSIONS,
  TOOLTIP_STYLE,
  TOOLTIP_LABEL_STYLE,
  GRID_PROPS,
  X_AXIS_PROPS,
  Y_AXIS_PROPS,
  LEGEND_PROPS,
  LINE_PROPS,
  formatAxisK,
} from './config'
import { useRevenueTrend } from '../../hooks'
import { formatCurrency } from '../../utils/formatters'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  date: string
  revenue: number
  orders: number
  prevRevenue?: number
  prevOrders?: number
}

// ─── Component ───────────────────────────────────────────────────────────────

export const RevenueTrendChart = memo(function RevenueTrendChart() {
  const { data, isLoading, error, refetch } = useRevenueTrend()

  const chartData = useMemo<ChartDataPoint[]>(() => {
    if (!data?.labels?.length) return []
    return data.labels.map((label, index) => ({
      date: label,
      revenue: data.revenue?.[index] ?? 0,
      orders: data.orders?.[index] ?? 0,
      prevRevenue: data.comparison?.revenue?.[index],
      prevOrders: data.comparison?.orders?.[index],
    }))
  }, [data])

  const hasComparison = data?.comparison?.revenue?.length ?? 0 > 0
  const isEmpty = !isLoading && chartData.length === 0

  return (
    <ChartContainer
      title="Revenue Trend"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xl"
      ariaLabel="Line chart showing revenue and orders over time"
    >
      <div style={{ height: 350 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData} margin={CHART_DIMENSIONS.margin.default}>
            <CartesianGrid {...GRID_PROPS} />
            <XAxis dataKey="date" {...X_AXIS_PROPS} />
            <YAxis
              yAxisId="revenue"
              {...Y_AXIS_PROPS}
              tickFormatter={formatAxisK}
            />
            <YAxis
              yAxisId="orders"
              orientation="right"
              {...Y_AXIS_PROPS}
            />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              labelStyle={TOOLTIP_LABEL_STYLE}
              formatter={(value, name) => {
                const numValue = Number(value) || 0
                if (name === 'revenue' || name === 'prevRevenue') {
                  return [formatCurrency(numValue), name === 'prevRevenue' ? 'Previous Revenue' : 'Revenue']
                }
                return [numValue, name === 'prevOrders' ? 'Previous Orders' : 'Orders']
              }}
            />
            <Legend {...LEGEND_PROPS} />
            {/* Current period lines */}
            <Line
              yAxisId="revenue"
              type="monotone"
              dataKey="revenue"
              name="Revenue"
              stroke={CHART_THEME.primary}
              {...LINE_PROPS}
              activeDot={{ r: 4, fill: CHART_THEME.primary }}
            />
            <Line
              yAxisId="orders"
              type="monotone"
              dataKey="orders"
              name="Orders"
              stroke={CHART_THEME.accent}
              {...LINE_PROPS}
              activeDot={{ r: 4, fill: CHART_THEME.accent }}
            />
            {/* Previous period lines (dashed) */}
            {hasComparison && (
              <>
                <Line
                  yAxisId="revenue"
                  type="monotone"
                  dataKey="prevRevenue"
                  name="Previous Revenue"
                  stroke={CHART_THEME.primary}
                  strokeDasharray="5 5"
                  strokeOpacity={0.5}
                  strokeWidth={1.5}
                  dot={false}
                  activeDot={{ r: 3, fill: CHART_THEME.primary, fillOpacity: 0.5 }}
                />
                <Line
                  yAxisId="orders"
                  type="monotone"
                  dataKey="prevOrders"
                  name="Previous Orders"
                  stroke={CHART_THEME.accent}
                  strokeDasharray="5 5"
                  strokeOpacity={0.5}
                  strokeWidth={1.5}
                  dot={false}
                  activeDot={{ r: 3, fill: CHART_THEME.accent, fillOpacity: 0.5 }}
                />
              </>
            )}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </ChartContainer>
  )
})
