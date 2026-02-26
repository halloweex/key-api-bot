import { memo } from 'react'
import { useTranslation } from 'react-i18next'
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
import { SourceChartTooltip } from './SourceChartTooltip'
import {
  CHART_DIMENSIONS,
  GRID_PROPS,
  X_AXIS_PROPS,
  Y_AXIS_PROPS,
  BAR_PROPS,
  PIE_PROPS,
  formatAxisK,
  formatPieLabel,
} from './config'
import { useSourceChartData } from '../../hooks'
import { formatCurrency } from '../../utils/formatters'

// ─── Component ───────────────────────────────────────────────────────────────

export const SalesBySourceChart = memo(function SalesBySourceChart() {
  const { t } = useTranslation()
  const { chartData, totalRevenue, isEmpty, isLoading, error, refetch } = useSourceChartData()

  return (
    <ChartContainer
      title={t('chart.salesBySource')}
      isLoading={isLoading}
      error={error}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="md"
      ariaLabel={t('chart.salesBySourceDesc')}
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
                content={<SourceChartTooltip showRevenue showOrders={false} />}
                cursor={{ fill: 'rgba(0, 0, 0, 0.04)' }}
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
                content={<SourceChartTooltip showRevenue indicatorShape="circle" />}
              />
            </PieChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Summary */}
      <div className="mt-4 pt-4 border-t border-slate-700">
        <div className="flex justify-between items-center">
          <span className="text-slate-400 text-sm">{t('chart.totalRevenue')}</span>
          <span className="text-white font-semibold">{formatCurrency(totalRevenue)}</span>
        </div>
      </div>
    </ChartContainer>
  )
})
