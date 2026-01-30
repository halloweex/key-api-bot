import { useMemo, memo, useState } from 'react'
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
  Cell,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import {
  CHART_THEME,
  TOOLTIP_STYLE,
  GRID_PROPS,
  X_AXIS_PROPS,
  Y_AXIS_PROPS,
} from './config'
import { useRevenueTrend } from '../../hooks'
import { useFilterStore } from '../../store/filterStore'
import { formatCurrency } from '../../utils/formatters'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  date: string
  shortDate: string
  revenue: number
  forecastRevenue: number
  orders: number
  prevRevenue: number
  prevOrders: number
  change: number
  changePercent: number
  isPeak: boolean
  peakLabel: string
  isCurrentMonth: boolean
  isForecast: boolean
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

  const displayRevenue = data.isForecast ? data.forecastRevenue : data.revenue
  const hasComparison = data.prevRevenue > 0
  const isPositive = data.change >= 0
  const changeColor = isPositive ? CHART_THEME.primary : CHART_THEME.danger
  const changeIcon = isPositive ? '↑' : '↓'

  return (
    <div
      style={{
        ...TOOLTIP_STYLE,
        padding: '10px 12px',
        minWidth: '180px',
        maxWidth: '280px',
      }}
    >
      <p style={{ fontWeight: 600, marginBottom: '10px', color: CHART_THEME.text, fontSize: '13px' }}>
        {data.date}
        {data.isForecast && (
          <span style={{ color: CHART_THEME.muted, fontWeight: 400, fontSize: '11px', marginLeft: '6px' }}>
            (predicted)
          </span>
        )}
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
            background: data.isForecast ? FORECAST_BAR_COLOR : CHART_THEME.primary,
          }} />
          <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>
            {data.isForecast ? 'Predicted' : periodLabels.current}
          </span>
        </div>
        <span style={{ fontWeight: 600, color: data.isForecast ? FORECAST_BAR_COLOR : CHART_THEME.primary }}>
          {formatCurrency(displayRevenue)}
        </span>
      </div>

      {/* Previous Period */}
      {hasComparison && data.prevRevenue > 0 && (
        <>
          <div style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: '8px'
          }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <div style={{
                width: '14px',
                height: '0px',
                borderTop: `2px dashed ${CHART_THEME.muted}`,
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
  hasPrevMonthDays: boolean
  hasForecast: boolean
}

function CustomLegend({ periodLabels, hasComparison, hasPrevMonthDays, hasForecast }: LegendProps) {
  // Get current month name for legend
  const currentMonthName = new Date().toLocaleString('en', { month: 'short' })

  return (
    <div style={{
      display: 'flex',
      justifyContent: 'center',
      flexWrap: 'wrap',
      gap: '16px',
      paddingTop: '8px',
      fontSize: '12px',
    }}>
      {/* Current month indicator */}
      <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
        <div style={{
          width: '14px',
          height: '14px',
          borderRadius: '3px',
          background: CHART_THEME.primary,
          flexShrink: 0,
        }} />
        <span style={{ color: CHART_THEME.text, fontWeight: 500, whiteSpace: 'nowrap' }}>
          {hasPrevMonthDays ? `${currentMonthName} (current month)` : periodLabels.current}
        </span>
      </div>
      {/* Previous month indicator - only show when there are prev month days */}
      {hasPrevMonthDays && (
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <div style={{
            width: '14px',
            height: '14px',
            borderRadius: '3px',
            background: PREV_MONTH_BAR_COLOR,
            flexShrink: 0,
          }} />
          <span style={{ color: CHART_THEME.muted, whiteSpace: 'nowrap' }}>Previous month</span>
        </div>
      )}
      {/* Forecast indicator */}
      {hasForecast && (
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <div style={{
            width: '14px',
            height: '14px',
            borderRadius: '3px',
            background: FORECAST_BAR_COLOR,
            opacity: 0.7,
            flexShrink: 0,
          }} />
          <span style={{ color: CHART_THEME.muted, whiteSpace: 'nowrap' }}>Predicted</span>
        </div>
      )}
      {/* Comparison period indicator */}
      {hasComparison && (
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <div style={{
            width: '16px',
            height: '0px',
            borderTop: `2px dashed ${CHART_THEME.muted}`,
            flexShrink: 0,
          }} />
          <span style={{ color: CHART_THEME.muted, whiteSpace: 'nowrap' }}>{periodLabels.previous}</span>
        </div>
      )}
    </div>
  )
}

// ─── Gradient Definitions ────────────────────────────────────────────────────

// Previous month bar color (lighter/muted version of primary)
const PREV_MONTH_BAR_COLOR = '#93c5fd' // Light blue (tailwind blue-300)

// Forecast bar color (same blue family, lighter)
const FORECAST_BAR_COLOR = '#60a5fa' // tailwind blue-400

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
      <linearGradient id="prevMonthBarGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={PREV_MONTH_BAR_COLOR} stopOpacity={0.9} />
        <stop offset="100%" stopColor={PREV_MONTH_BAR_COLOR} stopOpacity={0.7} />
      </linearGradient>
      <linearGradient id="forecastBarGradient" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stopColor={FORECAST_BAR_COLOR} stopOpacity={0.45} />
        <stop offset="100%" stopColor={FORECAST_BAR_COLOR} stopOpacity={0.25} />
      </linearGradient>
    </defs>
  )
}

// ─── Info Button ──────────────────────────────────────────────────────────────

function InfoButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className="text-slate-400 hover:text-slate-600 transition-colors"
      aria-label="Chart information"
    >
      <svg className="w-4 h-4" viewBox="0 0 24 24" fill="currentColor">
        <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 17h-2v-2h2v2zm2.07-7.75l-.9.92C13.45 12.9 13 13.5 13 15h-2v-.5c0-1.1.45-2.1 1.17-2.83l1.24-1.26c.37-.36.59-.86.59-1.41 0-1.1-.9-2-2-2s-2 .9-2 2H8c0-2.21 1.79-4 4-4s4 1.79 4 4c0 .88-.36 1.68-.93 2.25z"/>
      </svg>
    </button>
  )
}

function InfoTooltipContent({ onClose, children }: {
  onClose: () => void
  children: React.ReactNode
}) {
  return (
    <div className="absolute top-8 right-0 z-50 bg-slate-800 border border-slate-700 rounded-lg shadow-xl p-4 min-w-[220px] max-w-[300px]">
      <button
        onClick={onClose}
        className="absolute top-2 right-2 text-slate-400 hover:text-slate-200 text-lg leading-none"
        aria-label="Close"
      >
        ×
      </button>
      <h4 className="text-sm font-semibold text-slate-200 mb-2">Revenue Trend</h4>
      {children}
    </div>
  )
}

// ─── Compare Type Labels ──────────────────────────────────────────────────────

type CompareType = 'previous_period' | 'year_ago' | 'month_ago'

const COMPARE_TYPE_OPTIONS: { value: CompareType; label: string; shortLabel: string }[] = [
  { value: 'year_ago', label: 'Year over Year', shortLabel: 'Year ago' },
  { value: 'month_ago', label: 'Month over Month', shortLabel: 'Month ago' },
  { value: 'previous_period', label: 'Prior Period', shortLabel: 'Previous' },
]

// Get comparison label based on compare type
const getComparisonLabel = (compareType: CompareType, basePeriodLabel: string): string => {
  switch (compareType) {
    case 'year_ago':
      return 'Last Year'
    case 'month_ago':
      return 'Last Month'
    default:
      return basePeriodLabel
  }
}

// ─── Component ───────────────────────────────────────────────────────────────

export const RevenueTrendChart = memo(function RevenueTrendChart() {
  const [compareType, setCompareType] = useState<CompareType>('year_ago')
  const { data, isLoading, error, refetch } = useRevenueTrend(compareType)
  const { period } = useFilterStore()
  const [showInfo, setShowInfo] = useState(false)

  const basePeriodLabels = PERIOD_LABELS[period] || PERIOD_LABELS.custom

  // Create dynamic period labels based on compare type
  const periodLabels = useMemo(() => ({
    current: basePeriodLabels.current,
    previous: getComparisonLabel(compareType, basePeriodLabels.previous)
  }), [basePeriodLabels, compareType])

  // Get growth data from comparison
  const growthData = data?.comparison?.totals

  // Forecast data
  const forecast = data?.forecast

  const { chartData, hasComparison, hasPrevMonthDays, hasForecast } = useMemo(() => {
    if (!data?.labels?.length) {
      return { chartData: [], hasComparison: false, hasPrevMonthDays: false, hasForecast: false }
    }

    const hasComp = (data.comparison?.revenue?.length ?? 0) > 0

    // Find top 5 peak indices for labels
    const revenues = data.revenue ?? []
    const revenueWithIndex = revenues.map((rev, idx) => ({ rev, idx }))
    const topPeakIndices = new Set(
      revenueWithIndex
        .filter(item => item.rev > 0)
        .sort((a, b) => b.rev - a.rev)
        .slice(0, 5)
        .map(item => item.idx)
    )

    // Get current month for comparison (only relevant for last_28_days)
    // Month is 1-indexed in the date format (01 = January)
    const currentMonth = new Date().getMonth() + 1

    let prevMonthCount = 0

    const processed: ChartDataPoint[] = data.labels.map((label, index) => {
      const revenue = data.revenue?.[index] ?? 0
      const orders = data.orders?.[index] ?? 0
      const prevRevenue = data.comparison?.revenue?.[index] ?? 0
      const prevOrders = data.comparison?.orders?.[index] ?? 0

      const change = revenue - prevRevenue
      const changePercent = prevRevenue > 0 ? (change / prevRevenue) * 100 : 0

      // Format short date for x-axis
      const shortDate = label.length > 6 ? label.slice(0, 6) : label

      // Mark as peak if it's in top 5 revenues
      const isPeak = topPeakIndices.has(index)

      // Parse month from label (format: "dd.mm" like "21.01" for January 21st)
      // Check if this date is in current month
      let isCurrentMonth = true
      if (period === 'last_28_days') {
        // Extract month from "dd.mm" format
        const parts = label.split('.')
        if (parts.length >= 2) {
          const labelMonth = parseInt(parts[1], 10)
          // Check if the label is from current month
          if (labelMonth !== currentMonth) {
            isCurrentMonth = false
            prevMonthCount++
          }
        }
      }

      return {
        date: label,
        shortDate,
        revenue,
        forecastRevenue: 0,
        orders,
        prevRevenue,
        prevOrders,
        change,
        changePercent,
        isPeak,
        peakLabel: isPeak ? formatShortCurrency(revenue) : '',
        isCurrentMonth,
        isForecast: false,
      }
    })

    // Merge or append forecast days if available
    let forecastAppended = false
    if (forecast?.daily_predictions?.length) {
      // Build a lookup of existing labels for merging
      const labelIndex = new Map<string, number>()
      processed.forEach((p, idx) => labelIndex.set(p.date, idx))

      for (const pred of forecast.daily_predictions) {
        // Parse date to get dd.mm format (matching the label format)
        const parts = pred.date.split('-') // "2026-01-30"
        if (parts.length === 3) {
          const label = `${parts[2]}.${parts[1]}` // "30.01"
          const existingIdx = labelIndex.get(label)

          if (existingIdx !== undefined && processed[existingIdx].revenue === 0) {
            // Merge into existing zero-revenue data point
            processed[existingIdx].forecastRevenue = Math.round(pred.predicted_revenue)
            processed[existingIdx].isForecast = true
          } else if (existingIdx === undefined) {
            // Append as new data point (date not in main data)
            processed.push({
              date: label,
              shortDate: label,
              revenue: 0,
              forecastRevenue: Math.round(pred.predicted_revenue),
              orders: 0,
              prevRevenue: 0,
              prevOrders: 0,
              change: 0,
              changePercent: 0,
              isPeak: false,
              peakLabel: '',
              isCurrentMonth: true,
              isForecast: true,
            })
          }
          forecastAppended = true
        }
      }
    }

    return {
      chartData: processed,
      hasComparison: hasComp,
      hasPrevMonthDays: prevMonthCount > 0,
      hasForecast: forecastAppended,
    }
  }, [data, period, forecast])

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
      action={
        <div className="flex items-center gap-3">
          {/* Forecast Predicted Total Badge */}
          {forecast && !isLoading && (
            <div className="px-2.5 py-1 rounded-full text-xs font-semibold flex items-center gap-1 bg-blue-50 text-blue-600 border border-blue-200">
              <span>Predicted:</span>
              <span>{formatShortCurrency(forecast.predicted_total)}</span>
            </div>
          )}

          {/* Growth Delta Badge */}
          {growthData && !isLoading && (
            <div
              className={`px-2.5 py-1 rounded-full text-xs font-semibold flex items-center gap-1 ${
                growthData.growth_percent >= 0
                  ? 'bg-emerald-100 text-emerald-700'
                  : 'bg-red-100 text-red-700'
              }`}
            >
              <span>{growthData.growth_percent >= 0 ? '↑' : '↓'}</span>
              <span>{Math.abs(growthData.growth_percent).toFixed(1)}%</span>
            </div>
          )}

          {/* Compare Type Selector */}
          <select
            value={compareType}
            onChange={(e) => setCompareType(e.target.value as CompareType)}
            className="text-xs bg-slate-100 border-0 rounded-lg px-2.5 py-1.5 text-slate-600 font-medium cursor-pointer hover:bg-slate-200 transition-colors focus:ring-2 focus:ring-blue-500 focus:outline-none"
          >
            {COMPARE_TYPE_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                vs {opt.shortLabel}
              </option>
            ))}
          </select>

          {/* Info Button */}
          <div className="relative">
            <InfoButton onClick={() => setShowInfo(!showInfo)} />
            {showInfo && (
              <InfoTooltipContent onClose={() => setShowInfo(false)}>
                <div className="space-y-2">
                  <p className="text-xs text-slate-300">
                    <strong className="text-blue-400">Bars:</strong> Daily revenue for selected period.
                  </p>
                  {hasForecast && (
                    <p className="text-xs text-slate-300">
                      <strong style={{ color: FORECAST_BAR_COLOR }}>Lighter bars:</strong> ML-predicted revenue for remaining days.
                    </p>
                  )}
                  <p className="text-xs text-slate-300">
                    <strong className="text-slate-400">Dashed line:</strong> Comparison period (Year ago, Month ago, or Previous).
                  </p>
                  <p className="text-xs text-slate-300">
                    <strong className="text-emerald-400">Growth badge:</strong> Total revenue change vs comparison period.
                  </p>
                  <p className="text-xs text-slate-300">
                    <strong className="text-amber-400">Peak labels:</strong> Top 5 revenue days.
                  </p>
                </div>
              </InfoTooltipContent>
            )}
          </div>
        </div>
      }
    >
      <div className="flex flex-col h-[350px] sm:h-[400px] lg:h-[450px]">
        <div style={{ flex: 1, minHeight: 0 }}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart
            data={chartData}
            margin={{ top: 25, right: 15, left: 5, bottom: 10 }}

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
              hide={true}
              width={0}
              domain={[0, (dataMax: number) => Math.ceil(dataMax * 1.15 / 1000) * 1000]}
            />

            <Tooltip
              content={<CustomTooltip periodLabels={periodLabels} />}
              cursor={{ fill: 'rgba(0, 0, 0, 0.04)' }}
            />

            {/* Current period bars - different color for previous month days */}
            <Bar
              dataKey="revenue"
              name={periodLabels.current}
              stackId="revenue"
              fill={CHART_THEME.primary}
              radius={[4, 4, 0, 0]}
              maxBarSize={50}
            >
              {/* Conditional coloring for last_28_days period */}
              {chartData.map((entry, index) => (
                <Cell
                  key={`cell-${index}`}
                  fill={entry.isCurrentMonth ? CHART_THEME.primary : PREV_MONTH_BAR_COLOR}
                />
              ))}
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

            {/* Forecast bars — stacked on revenue so they don't halve bar width */}
            {hasForecast && (
              <Bar
                dataKey="forecastRevenue"
                name="Predicted"
                stackId="revenue"
                fill="url(#forecastBarGradient)"
                radius={[4, 4, 0, 0]}
                maxBarSize={50}
              />
            )}

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

            {/* Hide default legend, use custom */}
            <Legend content={() => null} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        {/* Custom Legend */}
        <CustomLegend periodLabels={periodLabels} hasComparison={hasComparison} hasPrevMonthDays={hasPrevMonthDays} hasForecast={hasForecast} />
      </div>
    </ChartContainer>
  )
})
