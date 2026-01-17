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
    }))
  }, [data])

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
      <div style={{ height: CHART_DIMENSIONS.height.xl }}>
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
              formatter={(value, name) => [
                name === 'revenue' ? formatCurrency(Number(value) || 0) : Number(value) || 0,
                name === 'revenue' ? 'Revenue' : 'Orders',
              ]}
            />
            <Legend {...LEGEND_PROPS} />
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
          </LineChart>
        </ResponsiveContainer>
      </div>
    </ChartContainer>
  )
})
