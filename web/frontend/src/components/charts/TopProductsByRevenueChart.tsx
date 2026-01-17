import { useMemo, memo } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LabelList,
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
  BAR_PROPS,
  wrapText,
} from './config'
import { useProductPerformance } from '../../hooks'
import { formatCurrency, formatNumber } from '../../utils/formatters'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  name: string
  lines: string[]
  fullName: string
  revenue: number
  quantity: number
}

// ─── Custom Y-Axis Tick ───────────────────────────────────────────────────────

interface CustomTickProps {
  x: number
  y: number
  payload: { value: string }
  tickData: Map<string, string[]>
}

const CustomYAxisTick = ({ x, y, payload, tickData }: CustomTickProps) => {
  const lines = tickData.get(payload.value) || [payload.value]

  return (
    <g transform={`translate(${x},${y})`}>
      {lines.map((line, index) => (
        <text
          key={index}
          x={-5}
          y={index * 12 - (lines.length - 1) * 6}
          textAnchor="end"
          fill={CHART_THEME.axis}
          fontSize={CHART_DIMENSIONS.fontSize.sm}
        >
          {line}
        </text>
      ))}
    </g>
  )
}

// ─── Component ───────────────────────────────────────────────────────────────

export const TopProductsByRevenueChart = memo(function TopProductsByRevenueChart() {
  const { data, isLoading, error, refetch } = useProductPerformance()

  const chartData = useMemo<ChartDataPoint[]>(() => {
    if (!data?.topByRevenue?.labels?.length) return []
    return data.topByRevenue.labels.map((label, index) => {
      const lines = wrapText(label, 18)
      return {
        name: lines.join(' '),
        lines,
        fullName: label || 'Unknown',
        revenue: data.topByRevenue.data?.[index] ?? 0,
        quantity: data.topByRevenue.quantities?.[index] ?? 0,
      }
    })
  }, [data])

  // Map for custom tick to look up wrapped lines
  const tickData = useMemo(() => {
    const map = new Map<string, string[]>()
    chartData.forEach((item) => map.set(item.name, item.lines))
    return map
  }, [chartData])

  const isEmpty = !isLoading && chartData.length === 0

  return (
    <ChartContainer
      title="Top 10 Products by Revenue"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xl"
      ariaLabel="Horizontal bar chart showing top 10 products by revenue"
    >
      <div style={{ height: CHART_DIMENSIONS.height.xl }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            layout="vertical"
            margin={CHART_DIMENSIONS.margin.withRightLabel}
          >
            <CartesianGrid {...GRID_PROPS} horizontal={false} />
            <XAxis type="number" {...X_AXIS_PROPS} />
            <YAxis
              type="category"
              dataKey="name"
              {...Y_AXIS_PROPS}
              width={180}
              tick={(props) => <CustomYAxisTick {...props} tickData={tickData} />}
            />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              labelStyle={TOOLTIP_LABEL_STYLE}
              formatter={(value, _name, props) => {
                const quantity = (props.payload as ChartDataPoint)?.quantity ?? 0
                return [
                  `${formatCurrency(Number(value) || 0)} (${formatNumber(quantity)} sold)`,
                  'Revenue',
                ]
              }}
              labelFormatter={(_label, payload) => {
                const item = payload?.[0]?.payload as ChartDataPoint | undefined
                return item?.fullName || String(_label)
              }}
            />
            <Bar
              dataKey="revenue"
              fill={CHART_THEME.accent}
              {...BAR_PROPS}
            >
              <LabelList
                dataKey="revenue"
                position="right"
                fill={CHART_THEME.label}
                fontSize={CHART_DIMENSIONS.fontSize.sm}
                formatter={(value: number) => formatCurrency(value)}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </ChartContainer>
  )
})
