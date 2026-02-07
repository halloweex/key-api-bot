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
  Y_AXIS_PROPS,
  BAR_PROPS,
  wrapText,
} from './config'
import { useTopProducts } from '../../hooks'
import { formatNumber } from '../../utils/formatters'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  name: string
  lines: string[]
  fullName: string
  quantity: number
  percentage: number
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

export const TopProductsChart = memo(function TopProductsChart() {
  const { data, isLoading, error, refetch } = useTopProducts()

  const chartData = useMemo<ChartDataPoint[]>(() => {
    if (!data?.labels?.length) return []
    return data.labels.map((label, index) => {
      const lines = wrapText(label, 14)
      return {
        name: lines.join(' '),  // Use joined text as unique key
        lines,
        fullName: label || 'Unknown',
        quantity: data.data?.[index] ?? 0,
        percentage: data.percentages?.[index] ?? 0,
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
      title="Top 10 Products by Quantity"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xxl"
      ariaLabel="Horizontal bar chart showing top 10 products by quantity sold"
    >
      <div className="h-[380px] sm:h-[400px] lg:h-[420px]">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            layout="vertical"
            margin={CHART_DIMENSIONS.margin.withRightLabel}
          >
            <CartesianGrid {...GRID_PROPS} horizontal={false} />
            <XAxis type="number" hide={true} />
            <YAxis
              type="category"
              dataKey="name"
              {...Y_AXIS_PROPS}
              width={100}
              tick={(props) => <CustomYAxisTick {...props} tickData={tickData} />}
            />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              labelStyle={TOOLTIP_LABEL_STYLE}
              formatter={(value, _name, props) => {
                const percentage = (props.payload as ChartDataPoint)?.percentage ?? 0
                return [`${formatNumber(Number(value) || 0)} (${percentage.toFixed(1)}%)`, 'Qty']
              }}
              labelFormatter={(_label, payload) => {
                const item = payload?.[0]?.payload as ChartDataPoint | undefined
                const name = item?.fullName || String(_label)
                return name.length > 40 ? name.slice(0, 40) + '...' : name
              }}
            />
            <Bar
              dataKey="quantity"
              fill={CHART_THEME.primary}
              {...BAR_PROPS}
            >
              <LabelList
                dataKey="percentage"
                position="right"
                fill={CHART_THEME.label}
                fontSize={CHART_DIMENSIONS.fontSize.sm}
                formatter={(value) => `${Number(value).toFixed(1)}%`}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </ChartContainer>
  )
})
