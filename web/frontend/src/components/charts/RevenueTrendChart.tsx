import { useMemo, memo } from 'react'
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  Cell,
  ReferenceLine,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import {
  CHART_THEME,
  CHART_DIMENSIONS,
  TOOLTIP_STYLE,
  GRID_PROPS,
  X_AXIS_PROPS,
  Y_AXIS_PROPS,
  formatAxisK,
} from './config'
import { useRevenueTrend } from '../../hooks'
import { useFilterStore } from '../../store/filterStore'
import { formatCurrency } from '../../utils/formatters'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  date: string
  shortDate: string
  revenue: number
  orders: number
  prevRevenue: number
  prevOrders: number
  change: number
  changePercent: number
  cumulative: number
}

// ─── Period Labels ───────────────────────────────────────────────────────────

const PERIOD_LABELS: Record<string, { current: string; previous: string }> = {
  today: { current: 'Today', previous: 'Yesterday' },
  yesterday: { current: 'Yesterday', previous: 'Day Before' },
  week: { current: 'This Week', previous: 'Last Week' },
  last_week: { current: 'Last Week', previous: 'Week Before' },
  month: { current: 'This Month', previous: 'Last Month' },
  last_month: { current: 'Last Month', previous: 'Month Before' },
  custom: { current: 'Selected Period', previous: 'Previous Period' },
}

// ─── Custom Tooltip ──────────────────────────────────────────────────────────

interface TooltipProps {
  active?: boolean
  payload?: Array<{
    value: number
    dataKey: string
    payload: ChartDataPoint
  }>
  label?: string
  periodLabels: { current: string; previous: string }
}

function CustomTooltip({ active, payload, periodLabels }: TooltipProps) {
  if (!active || !payload?.length) return null

  const data = payload[0]?.payload
  if (!data) return null

  const isPositive = data.change >= 0
  const changeColor = isPositive ? CHART_THEME.success : CHART_THEME.danger
  const changeIcon = isPositive ? '↑' : '↓'

  return (
    <div
      style={{
        ...TOOLTIP_STYLE,
        padding: '12px 16px',
        minWidth: '200px',
      }}
    >
      <p style={{ fontWeight: 600, marginBottom: '8px', color: CHART_THEME.text }}>
        {data.date}
      </p>

      {/* Current Period */}
      <div style={{ marginBottom: '8px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>
            {periodLabels.current}
          </span>
          <span style={{ fontWeight: 600, color: CHART_THEME.primary }}>
            {formatCurrency(data.revenue)}
          </span>
        </div>
        <div style={{ fontSize: '11px', color: CHART_THEME.muted }}>
          {data.orders} orders
        </div>
      </div>

      {/* Previous Period */}
      {data.prevRevenue > 0 && (
        <div style={{ marginBottom: '8px', paddingBottom: '8px', borderBottom: `1px solid ${CHART_THEME.border}` }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>
              {periodLabels.previous}
            </span>
            <span style={{ fontWeight: 500, color: CHART_THEME.muted }}>
              {formatCurrency(data.prevRevenue)}
            </span>
          </div>
          <div style={{ fontSize: '11px', color: CHART_THEME.muted }}>
            {data.prevOrders} orders
          </div>
        </div>
      )}

      {/* Change */}
      {data.prevRevenue > 0 && (
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>Change</span>
          <span style={{ fontWeight: 600, color: changeColor }}>
            {changeIcon} {Math.abs(data.changePercent).toFixed(1)}%
            <span style={{ fontSize: '11px', marginLeft: '4px', opacity: 0.8 }}>
              ({isPositive ? '+' : ''}{formatCurrency(data.change)})
            </span>
          </span>
        </div>
      )}
    </div>
  )
}

// ─── Custom Legend ───────────────────────────────────────────────────────────

interface LegendProps {
  periodLabels: { current: string; previous: string }
  hasComparison: boolean
}

function CustomLegend({ periodLabels, hasComparison }: LegendProps) {
  return (
    <div style={{
      display: 'flex',
      justifyContent: 'center',
      gap: '24px',
      marginTop: '8px',
      fontSize: '12px',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
        <div style={{
          width: '12px',
          height: '12px',
          borderRadius: '2px',
          background: CHART_THEME.primary,
        }} />
        <span style={{ color: CHART_THEME.text }}>{periodLabels.current}</span>
      </div>
      {hasComparison && (
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <div style={{
            width: '12px',
            height: '12px',
            borderRadius: '2px',
            background: `${CHART_THEME.primary}40`,
            border: `1px dashed ${CHART_THEME.primary}`,
          }} />
          <span style={{ color: CHART_THEME.muted }}>{periodLabels.previous}</span>
        </div>
      )}
      <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
        <div style={{
          width: '16px',
          height: '2px',
          background: CHART_THEME.accent,
        }} />
        <span style={{ color: CHART_THEME.text }}>Cumulative</span>
      </div>
    </div>
  )
}

// ─── Bar Shape with Gradient ─────────────────────────────────────────────────

function GradientDefs() {
  return (
    <defs>
      <linearGradient id="currentBarGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={CHART_THEME.primary} stopOpacity={1} />
        <stop offset="100%" stopColor={CHART_THEME.primary} stopOpacity={0.7} />
      </linearGradient>
      <linearGradient id="prevBarGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={CHART_THEME.primary} stopOpacity={0.35} />
        <stop offset="100%" stopColor={CHART_THEME.primary} stopOpacity={0.15} />
      </linearGradient>
      <linearGradient id="cumulativeGradient" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0%" stopColor={CHART_THEME.accent} stopOpacity={0.3} />
        <stop offset="50%" stopColor={CHART_THEME.accent} stopOpacity={1} />
        <stop offset="100%" stopColor={CHART_THEME.accent} stopOpacity={0.3} />
      </linearGradient>
    </defs>
  )
}

// ─── Component ───────────────────────────────────────────────────────────────

export const RevenueTrendChart = memo(function RevenueTrendChart() {
  const { data, isLoading, error, refetch } = useRevenueTrend()
  const { period } = useFilterStore()

  const periodLabels = PERIOD_LABELS[period] || PERIOD_LABELS.custom

  const { chartData, hasComparison, avgRevenue } = useMemo(() => {
    if (!data?.labels?.length) {
      return { chartData: [], hasComparison: false, avgRevenue: 0 }
    }

    const hasComp = (data.comparison?.revenue?.length ?? 0) > 0
    let cumulative = 0
    let totalRevenue = 0

    const processed = data.labels.map((label, index) => {
      const revenue = data.revenue?.[index] ?? 0
      const orders = data.orders?.[index] ?? 0
      const prevRevenue = data.comparison?.revenue?.[index] ?? 0
      const prevOrders = data.comparison?.orders?.[index] ?? 0

      cumulative += revenue
      totalRevenue += revenue

      const change = revenue - prevRevenue
      const changePercent = prevRevenue > 0 ? (change / prevRevenue) * 100 : 0

      // Format short date for x-axis (e.g., "Jan 15" or "Mon")
      const shortDate = label.length > 6 ? label.slice(0, 6) : label

      return {
        date: label,
        shortDate,
        revenue,
        orders,
        prevRevenue,
        prevOrders,
        change,
        changePercent,
        cumulative,
      }
    })

    const avg = totalRevenue / processed.length

    return {
      chartData: processed,
      hasComparison: hasComp,
      avgRevenue: avg,
    }
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
      ariaLabel="Combo chart showing revenue comparison between current and previous period"
    >
      <div style={{ height: 380 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart
            data={chartData}
            margin={{ top: 20, right: 30, left: 10, bottom: 20 }}
            barGap={2}
            barCategoryGap="20%"
          >
            <GradientDefs />
            <CartesianGrid {...GRID_PROPS} vertical={false} />

            <XAxis
              dataKey="shortDate"
              {...X_AXIS_PROPS}
              interval={0}
              angle={chartData.length > 10 ? -45 : 0}
              textAnchor={chartData.length > 10 ? 'end' : 'middle'}
              height={chartData.length > 10 ? 60 : 30}
            />

            {/* Primary Y-axis for revenue */}
            <YAxis
              yAxisId="revenue"
              {...Y_AXIS_PROPS}
              tickFormatter={formatAxisK}
              width={CHART_DIMENSIONS.yAxisWidth.sm}
            />

            {/* Secondary Y-axis for cumulative */}
            <YAxis
              yAxisId="cumulative"
              orientation="right"
              {...Y_AXIS_PROPS}
              tickFormatter={formatAxisK}
              width={CHART_DIMENSIONS.yAxisWidth.sm}
              stroke={CHART_THEME.accent}
            />

            <Tooltip
              content={<CustomTooltip periodLabels={periodLabels} />}
              cursor={{ fill: 'rgba(0, 0, 0, 0.04)' }}
            />

            {/* Average reference line */}
            {avgRevenue > 0 && (
              <ReferenceLine
                yAxisId="revenue"
                y={avgRevenue}
                stroke={CHART_THEME.muted}
                strokeDasharray="4 4"
                strokeWidth={1}
              >
              </ReferenceLine>
            )}

            {/* Previous period bars (background) */}
            {hasComparison && (
              <Bar
                yAxisId="revenue"
                dataKey="prevRevenue"
                name="Previous"
                fill="url(#prevBarGradient)"
                radius={[4, 4, 0, 0]}
                maxBarSize={40}
              />
            )}

            {/* Current period bars (foreground) */}
            <Bar
              yAxisId="revenue"
              dataKey="revenue"
              name="Current"
              fill="url(#currentBarGradient)"
              radius={[4, 4, 0, 0]}
              maxBarSize={40}
            >
              {chartData.map((entry, index) => {
                // Highlight bars that beat previous period
                const beatsPrevious = entry.revenue > entry.prevRevenue
                return (
                  <Cell
                    key={`cell-${index}`}
                    fill={beatsPrevious && hasComparison ? `url(#currentBarGradient)` : `url(#currentBarGradient)`}
                    stroke={beatsPrevious && hasComparison ? CHART_THEME.success : 'none'}
                    strokeWidth={beatsPrevious && hasComparison ? 1 : 0}
                  />
                )
              })}
            </Bar>

            {/* Cumulative trend line */}
            <Line
              yAxisId="cumulative"
              type="monotone"
              dataKey="cumulative"
              name="Cumulative"
              stroke={CHART_THEME.accent}
              strokeWidth={2.5}
              dot={false}
              activeDot={{
                r: 5,
                fill: CHART_THEME.accent,
                stroke: '#fff',
                strokeWidth: 2,
              }}
            />

            {/* Hide default legend, use custom */}
            <Legend content={() => null} />
          </ComposedChart>
        </ResponsiveContainer>

        {/* Custom Legend */}
        <CustomLegend periodLabels={periodLabels} hasComparison={hasComparison} />
      </div>
    </ChartContainer>
  )
})
