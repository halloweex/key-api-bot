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
  LabelList,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import { SourceChartTooltip } from './SourceChartTooltip'
import {
  CHART_THEME,
  CHART_DIMENSIONS,
  GRID_PROPS,
  Y_AXIS_PROPS,
  BAR_PROPS,
} from './config'
import { useSourceChartData } from '../../hooks'
import { formatNumber } from '../../utils/formatters'

// ─── Component ───────────────────────────────────────────────────────────────

export const OrdersBySourceChart = memo(function OrdersBySourceChart() {
  const { chartData: rawData, totalOrders, isEmpty, isLoading, error, refetch } = useSourceChartData()

  // Add label for bar chart
  const chartData = useMemo(() => {
    return rawData.map(item => ({
      ...item,
      label: `${formatNumber(item.orders)} (${item.ordersPercent.toFixed(0)}%)`,
    }))
  }, [rawData])

  return (
    <ChartContainer
      title="Orders by Source"
      isLoading={isLoading}
      error={error}
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
            margin={{ left: 20, right: 80 }}
          >
            <CartesianGrid {...GRID_PROPS} horizontal={false} />
            <XAxis
              type="number"
              hide={true}
            />
            <YAxis
              type="category"
              dataKey="name"
              {...Y_AXIS_PROPS}
              width={CHART_DIMENSIONS.yAxisWidth.md}
            />
            <Tooltip
              content={<SourceChartTooltip showRevenue={false} showOrders />}
              cursor={{ fill: 'rgba(0, 0, 0, 0.04)' }}
            />
            <Bar dataKey="orders" {...BAR_PROPS}>
              {chartData.map((entry, index) => (
                <Cell key={`bar-${index}`} fill={entry.color} />
              ))}
              <LabelList
                dataKey="label"
                position="right"
                style={{
                  fill: CHART_THEME.text,
                  fontSize: 11,
                  fontWeight: 500,
                }}
              />
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
