import { useMemo, memo } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
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
  PIE_PROPS,
  formatAxisK,
  formatPieLabel,
} from './config'
import { useSalesBySource } from '../../hooks'
import { formatCurrency } from '../../utils/formatters'
import { SOURCE_COLORS } from '../../utils/colors'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  name: string
  revenue: number
  orders: number
  color: string
  [key: string]: string | number  // Recharts compatibility
}

// ─── Component ───────────────────────────────────────────────────────────────

export const SalesBySourceChart = memo(function SalesBySourceChart() {
  const { data, isLoading, error, refetch } = useSalesBySource()

  const chartData = useMemo<ChartDataPoint[]>(() => {
    if (!data?.labels?.length) return []
    return data.labels.map((label, index) => ({
      name: label,
      revenue: data.revenue?.[index] ?? 0,
      orders: data.orders?.[index] ?? 0,
      color: data.backgroundColor?.[index] ?? SOURCE_COLORS[index % 3] ?? '#2563EB',
    }))
  }, [data])

  const totalRevenue = useMemo(
    () => chartData.reduce((sum, item) => sum + item.revenue, 0),
    [chartData]
  )

  const isEmpty = !isLoading && chartData.length === 0

  return (
    <ChartContainer
      title="Sales by Source"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="md"
      ariaLabel="Bar and pie charts showing sales distribution by source"
    >
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        {/* Bar Chart */}
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
                tickFormatter={formatAxisK}
              />
              <YAxis
                type="category"
                dataKey="name"
                {...Y_AXIS_PROPS}
                width={CHART_DIMENSIONS.yAxisWidth.md}
              />
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                labelStyle={{ color: '#F3F4F6' }}
                formatter={(value) => [formatCurrency(Number(value) || 0), 'Revenue']}
              />
              <Bar dataKey="revenue" {...BAR_PROPS}>
                {chartData.map((entry, index) => (
                  <Cell key={`bar-${index}`} fill={entry.color} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>

        {/* Pie Chart */}
        <div style={{ height: CHART_DIMENSIONS.height.md }}>
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={chartData}
                cx="50%"
                cy="50%"
                innerRadius={50}
                outerRadius={80}
                dataKey="revenue"
                nameKey="name"
                label={({ name, percent }) =>
                  `${name}: ${formatPieLabel(percent ?? 0)}`
                }
                {...PIE_PROPS}
              >
                {chartData.map((entry, index) => (
                  <Cell key={`pie-${index}`} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip
                contentStyle={TOOLTIP_STYLE}
                formatter={(value) => [formatCurrency(Number(value) || 0), 'Revenue']}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Summary */}
      <div className="mt-4 pt-4 border-t border-slate-700">
        <div className="flex justify-between items-center">
          <span className="text-slate-400 text-sm">Total Revenue</span>
          <span className="text-white font-semibold">{formatCurrency(totalRevenue)}</span>
        </div>
      </div>
    </ChartContainer>
  )
})
