import { useMemo, memo } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import {
  CHART_DIMENSIONS,
  TOOLTIP_STYLE,
  GRID_PROPS,
  X_AXIS_PROPS,
  Y_AXIS_PROPS,
  BAR_PROPS,
} from './config'
import { useSalesBySource } from '../../hooks'
import { formatNumber } from '../../utils/formatters'
import { SOURCE_COLORS } from '../../utils/colors'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  name: string
  orders: number
  color: string
}

// ─── Component ───────────────────────────────────────────────────────────────

export const OrdersBySourceChart = memo(function OrdersBySourceChart() {
  const { data, isLoading, error, refetch } = useSalesBySource()

  const chartData = useMemo<ChartDataPoint[]>(() => {
    if (!data?.labels?.length) return []
    return data.labels.map((label, index) => ({
      name: label,
      orders: data.orders?.[index] ?? 0,
      color: data.backgroundColor?.[index] ?? SOURCE_COLORS[index % 3] ?? '#2563EB',
    }))
  }, [data])

  const totalOrders = useMemo(
    () => chartData.reduce((sum, item) => sum + item.orders, 0),
    [chartData]
  )

  const isEmpty = !isLoading && chartData.length === 0

  return (
    <ChartContainer
      title="Orders by Source"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="md"
      ariaLabel="Bar chart showing orders distribution by source"
    >
      <div style={{ height: CHART_DIMENSIONS.height.md }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            layout="vertical"
            margin={{ left: 20, right: 20 }}
          >
            <CartesianGrid {...GRID_PROPS} horizontal={false} />
            <XAxis
              type="number"
              {...X_AXIS_PROPS}
            />
            <YAxis
              type="category"
              dataKey="name"
              {...Y_AXIS_PROPS}
              width={CHART_DIMENSIONS.yAxisWidth.md}
            />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              formatter={(value) => [formatNumber(Number(value) || 0), 'Orders']}
            />
            <Bar dataKey="orders" {...BAR_PROPS}>
              {chartData.map((entry, index) => (
                <Cell key={`bar-${index}`} fill={entry.color} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Summary */}
      <div className="mt-4 pt-4 border-t border-slate-200">
        <div className="flex justify-between items-center">
          <span className="text-slate-500 text-sm">Total Orders</span>
          <span className="text-slate-900 font-semibold">{formatNumber(totalOrders)}</span>
        </div>
      </div>
    </ChartContainer>
  )
})
