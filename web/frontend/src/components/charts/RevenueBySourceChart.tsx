import { useMemo, memo } from 'react'
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import {
  CHART_DIMENSIONS,
  TOOLTIP_STYLE,
  PIE_PROPS,
  formatPieLabel,
} from './config'
import { useSalesBySource } from '../../hooks'
import { formatCurrency } from '../../utils/formatters'
import { SOURCE_COLORS } from '../../utils/colors'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  name: string
  revenue: number
  color: string
  [key: string]: string | number
}

// ─── Component ───────────────────────────────────────────────────────────────

export const RevenueBySourceChart = memo(function RevenueBySourceChart() {
  const { data, isLoading, error, refetch } = useSalesBySource()

  const chartData = useMemo<ChartDataPoint[]>(() => {
    if (!data?.labels?.length) return []
    return data.labels.map((label, index) => ({
      name: label,
      revenue: data.revenue?.[index] ?? 0,
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
      title="Revenue by Source"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="md"
      ariaLabel="Pie chart showing revenue distribution by source"
    >
      <div style={{ height: CHART_DIMENSIONS.height.md }}>
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={chartData}
              cx="50%"
              cy="50%"
              innerRadius={50}
              outerRadius={90}
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

      {/* Summary */}
      <div className="mt-4 pt-4 border-t border-slate-200">
        <div className="flex justify-between items-center">
          <span className="text-slate-500 text-sm">Total Revenue</span>
          <span className="text-slate-900 font-semibold">{formatCurrency(totalRevenue)}</span>
        </div>
      </div>
    </ChartContainer>
  )
})
