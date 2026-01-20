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
  LabelList,
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
  isPeak: boolean
  peakLabel: string
}

// ─── Label Formatter ──────────────────────────────────────────────────────────

const formatShortCurrency = (value: number): string => {
  if (value >= 1000000) {
    return `₴${(value / 1000000).toFixed(1)}M`
  }
  if (value >= 1000) {
    return `₴${(value / 1000).toFixed(0)}K`
  }
  return `₴${value}`
}

// ─── Period Labels ───────────────────────────────────────────────────────────

const PERIOD_LABELS: Record<string, { current: string; previous: string }> = {
  today: { current: 'Today', previous: 'Yesterday' },
  yesterday: { current: 'Yesterday', previous: 'Day Before' },
  week: { current: 'This Week', previous: 'Last Week' },
  last_week: { current: 'Last Week', previous: 'Week Before' },
  month: { current: 'This Month', previous: 'Last Month' },
  last_month: { current: 'Last Month', previous: 'Month Before' },
  last_7_days: { current: 'Last 7 Days', previous: 'Previous 7 Days' },
  last_28_days: { current: 'Last 28 Days', previous: 'Previous 28 Days' },
  custom: { current: 'Selected', previous: 'Previous' },
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

  const hasComparison = data.prevRevenue > 0
  const isPositive = data.change >= 0
  const changeColor = isPositive ? CHART_THEME.primary : CHART_THEME.danger
  const changeIcon = isPositive ? '↑' : '↓'

  return (
    <div
      style={{
        ...TOOLTIP_STYLE,
        padding: '12px 16px',
        minWidth: '220px',
      }}
    >
      <p style={{ fontWeight: 600, marginBottom: '10px', color: CHART_THEME.text, fontSize: '13px' }}>
        {data.date}
      </p>

      {/* Current Period */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '6px',
        paddingBottom: hasComparison ? '6px' : '0',
        borderBottom: hasComparison ? `1px solid ${CHART_THEME.border}` : 'none'
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div style={{
            width: '12px',
            height: '12px',
            borderRadius: '3px',
            background: CHART_THEME.primary,
          }} />
          <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>
            {periodLabels.current}
          </span>
        </div>
        <span style={{ fontWeight: 600, color: CHART_THEME.primary }}>
          {formatCurrency(data.revenue)}
        </span>
      </div>

      {/* Previous Period */}
      {hasComparison && (
        <>
          <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: '8px'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <div style={{
                width: '12px',
                height: '12px',
                borderRadius: '3px',
                background: CHART_THEME.muted,
                opacity: 0.35,
              }} />
              <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>
                {periodLabels.previous}
              </span>
            </div>
            <span style={{ fontWeight: 500, color: CHART_THEME.muted }}>
              {formatCurrency(data.prevRevenue)}
            </span>
          </div>

          {/* Change Indicator */}
          <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            background: isPositive ? 'rgba(37, 99, 235, 0.1)' : 'rgba(239, 68, 68, 0.1)',
            padding: '6px 8px',
            borderRadius: '6px',
            marginTop: '4px'
          }}>
            <span style={{ color: CHART_THEME.text, fontSize: '12px', fontWeight: 500 }}>
              vs Previous
            </span>
            <span style={{ fontWeight: 700, color: changeColor, fontSize: '13px' }}>
              {changeIcon} {Math.abs(data.changePercent).toFixed(1)}%
            </span>
          </div>
        </>
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
      flexWrap: 'wrap',
      gap: '16px',
      paddingTop: '8px',
      fontSize: '12px',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
        <div style={{
          width: '14px',
          height: '14px',
          borderRadius: '3px',
          background: CHART_THEME.primary,
          flexShrink: 0,
        }} />
        <span style={{ color: CHART_THEME.text, fontWeight: 500, whiteSpace: 'nowrap' }}>{periodLabels.current}</span>
      </div>
      {hasComparison && (
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <div style={{
            width: '14px',
            height: '14px',
            borderRadius: '3px',
            background: CHART_THEME.muted,
            opacity: 0.35,
            flexShrink: 0,
          }} />
          <span style={{ color: CHART_THEME.muted, whiteSpace: 'nowrap' }}>{periodLabels.previous}</span>
        </div>
      )}
    </div>
  )
}

// ─── Gradient Definitions ────────────────────────────────────────────────────

function GradientDefs() {
  return (
    <defs>
      <linearGradient id="currentPeriodGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="5%" stopColor={CHART_THEME.primary} stopOpacity={0.3} />
        <stop offset="95%" stopColor={CHART_THEME.primary} stopOpacity={0.02} />
      </linearGradient>
      <linearGradient id="currentBarGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={CHART_THEME.primary} stopOpacity={1} />
        <stop offset="100%" stopColor={CHART_THEME.primary} stopOpacity={0.85} />
      </linearGradient>
      <linearGradient id="prevBarGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={CHART_THEME.muted} stopOpacity={0.5} />
        <stop offset="100%" stopColor={CHART_THEME.muted} stopOpacity={0.25} />
      </linearGradient>
    </defs>
  )
}

// ─── Component ───────────────────────────────────────────────────────────────

export const RevenueTrendChart = memo(function RevenueTrendChart() {
  const { data, isLoading, error, refetch } = useRevenueTrend()
  const { period } = useFilterStore()

  const periodLabels = PERIOD_LABELS[period] || PERIOD_LABELS.custom

  const { chartData, hasComparison, maxRevenue } = useMemo(() => {
    if (!data?.labels?.length) {
      return { chartData: [], hasComparison: false, maxRevenue: 0 }
    }

    const hasComp = (data.comparison?.revenue?.length ?? 0) > 0

    // Find max revenue for peak detection
    const revenues = data.revenue ?? []
    const maxRev = Math.max(...revenues, 0)

    const processed = data.labels.map((label, index) => {
      const revenue = data.revenue?.[index] ?? 0
      const orders = data.orders?.[index] ?? 0
      const prevRevenue = data.comparison?.revenue?.[index] ?? 0
      const prevOrders = data.comparison?.orders?.[index] ?? 0

      const change = revenue - prevRevenue
      const changePercent = prevRevenue > 0 ? (change / prevRevenue) * 100 : 0

      // Format short date for x-axis
      const shortDate = label.length > 6 ? label.slice(0, 6) : label

      // Mark as peak if it's the maximum value (or within 5% of max for multiple peaks)
      const isPeak = revenue > 0 && revenue >= maxRev * 0.95

      return {
        date: label,
        shortDate,
        revenue,
        orders,
        prevRevenue,
        prevOrders,
        change,
        changePercent,
        isPeak,
        peakLabel: isPeak ? formatShortCurrency(revenue) : '',
      }
    })

    return {
      chartData: processed,
      hasComparison: hasComp,
      maxRevenue: maxRev,
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
      ariaLabel="Chart showing revenue comparison between current and previous period"
    >
      <div style={{ display: 'flex', flexDirection: 'column', height: 350 }}>
        <div style={{ flex: 1, minHeight: 0 }}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart
            data={chartData}
            margin={{ top: 25, right: 20, left: 10, bottom: 10 }}
            barGap={0}
            barCategoryGap="8%"
          >
            <GradientDefs />
            <CartesianGrid {...GRID_PROPS} vertical={false} />

            <XAxis
              dataKey="shortDate"
              {...X_AXIS_PROPS}
              interval={chartData.length > 14 ? Math.floor(chartData.length / 7) : 0}
              angle={chartData.length > 20 ? -45 : 0}
              textAnchor={chartData.length > 20 ? 'end' : 'middle'}
              height={chartData.length > 20 ? 50 : 30}
            />

            <YAxis
              {...Y_AXIS_PROPS}
              tickFormatter={formatAxisK}
              width={CHART_DIMENSIONS.yAxisWidth.sm}
              domain={[0, (dataMax: number) => Math.ceil(dataMax * 1.15 / 1000) * 1000]}
            />

            <Tooltip
              content={<CustomTooltip periodLabels={periodLabels} />}
              cursor={{ fill: 'rgba(0, 0, 0, 0.04)' }}
            />

            {/* Previous period bars (background, lighter) */}
            {hasComparison && (
              <Bar
                dataKey="prevRevenue"
                name={periodLabels.previous}
                fill={CHART_THEME.muted}
                fillOpacity={0.35}
                radius={[4, 4, 0, 0]}
                maxBarSize={50}
              />
            )}

            {/* Current period bars (foreground, solid blue) */}
            <Bar
              dataKey="revenue"
              name={periodLabels.current}
              fill={CHART_THEME.primary}
              radius={[4, 4, 0, 0]}
              maxBarSize={50}
            >
              {/* Peak value labels */}
              <LabelList
                dataKey="peakLabel"
                position="top"
                style={{
                  fill: CHART_THEME.primary,
                  fontSize: 11,
                  fontWeight: 600,
                }}
              />
            </Bar>

            {/* Previous period trend line (dashed) */}
            {hasComparison && (
              <Line
                type="monotone"
                dataKey="prevRevenue"
                name={`${periodLabels.previous} Trend`}
                stroke={CHART_THEME.muted}
                strokeWidth={2}
                strokeDasharray="5 5"
                dot={false}
                activeDot={{ r: 4, fill: CHART_THEME.muted, stroke: '#fff', strokeWidth: 2 }}
              />
            )}

            {/* Current period trend line (solid) */}
            <Line
              type="monotone"
              dataKey="revenue"
              name={`${periodLabels.current} Trend`}
              stroke="#1d4ed8"
              strokeWidth={2.5}
              dot={false}
              activeDot={{ r: 5, fill: CHART_THEME.primary, stroke: '#fff', strokeWidth: 2 }}
            />

            {/* Hide default legend, use custom */}
            <Legend content={() => null} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        {/* Custom Legend */}
        <CustomLegend periodLabels={periodLabels} hasComparison={hasComparison} />
      </div>
    </ChartContainer>
  )
})
