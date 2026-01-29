import { memo } from 'react'
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import { SourceChartTooltip } from './SourceChartTooltip'
import {
  CHART_DIMENSIONS,
  PIE_PROPS,
  formatPieLabel,
} from './config'
import { useSourceChartData } from '../../hooks'
import { formatCurrency } from '../../utils/formatters'

// ─── Component ───────────────────────────────────────────────────────────────

export const RevenueBySourceChart = memo(function RevenueBySourceChart() {
  const { chartData, totalRevenue, isEmpty, isLoading, error, refetch } = useSourceChartData()

  return (
    <ChartContainer
      title="Revenue by Source"
      isLoading={isLoading}
      error={error}
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
              content={<SourceChartTooltip showRevenue indicatorShape="circle" />}
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
