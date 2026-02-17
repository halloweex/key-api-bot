import { memo, useMemo } from 'react'
import {
  ComposedChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import { ChartContainer } from '../charts/ChartContainer'
import {
  CHART_THEME,
  TOOLTIP_STYLE,
  GRID_PROPS,
  X_AXIS_PROPS,
} from '../charts/config'
import { useTrafficTrend } from '../../hooks/useApi'
import { formatCurrency, formatNumber } from '../../utils/formatters'

// ─── Colors ───────────────────────────────────────────────────────────────────

const PAID_COLOR = '#2563EB'      // blue-600
const ORGANIC_COLOR = '#10B981'   // emerald-500
const OTHER_COLOR = '#94A3B8'     // slate-400

// ─── Types ────────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  date: string
  shortDate: string
  paidRevenue: number
  organicRevenue: number
  otherRevenue: number
  paidOrders: number
  organicOrders: number
  otherOrders: number
  total: number
}

// ─── Custom Tooltip ───────────────────────────────────────────────────────────

function CustomTooltip({
  active,
  payload,
}: {
  active?: boolean
  payload?: Array<{ payload: ChartDataPoint }>
}) {
  if (!active || !payload?.length) return null

  const data = payload[0]?.payload
  if (!data) return null

  return (
    <div style={{ ...TOOLTIP_STYLE, minWidth: '180px' }}>
      <p style={{ fontWeight: 600, marginBottom: '10px', color: CHART_THEME.text }}>
        {data.date}
      </p>

      {/* Paid section */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '6px',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div style={{
            width: '12px',
            height: '12px',
            borderRadius: '3px',
            background: PAID_COLOR,
          }} />
          <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>Paid</span>
        </div>
        <div style={{ textAlign: 'right' }}>
          <div style={{ fontWeight: 600, color: PAID_COLOR }}>
            {formatCurrency(data.paidRevenue)}
          </div>
          <div style={{ fontSize: '10px', color: CHART_THEME.muted }}>
            {formatNumber(data.paidOrders)} orders
          </div>
        </div>
      </div>

      {/* Organic section */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '6px',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div style={{
            width: '12px',
            height: '12px',
            borderRadius: '3px',
            background: ORGANIC_COLOR,
          }} />
          <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>Organic</span>
        </div>
        <div style={{ textAlign: 'right' }}>
          <div style={{ fontWeight: 600, color: ORGANIC_COLOR }}>
            {formatCurrency(data.organicRevenue)}
          </div>
          <div style={{ fontSize: '10px', color: CHART_THEME.muted }}>
            {formatNumber(data.organicOrders)} orders
          </div>
        </div>
      </div>

      {/* Other section */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '8px',
        paddingBottom: '8px',
        borderBottom: `1px solid ${CHART_THEME.border}`,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div style={{
            width: '12px',
            height: '12px',
            borderRadius: '3px',
            background: OTHER_COLOR,
          }} />
          <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>Other</span>
        </div>
        <div style={{ textAlign: 'right' }}>
          <div style={{ fontWeight: 600, color: OTHER_COLOR }}>
            {formatCurrency(data.otherRevenue)}
          </div>
          <div style={{ fontSize: '10px', color: CHART_THEME.muted }}>
            {formatNumber(data.otherOrders)} orders
          </div>
        </div>
      </div>

      {/* Total */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        background: 'rgba(0, 0, 0, 0.03)',
        padding: '6px 8px',
        borderRadius: '6px',
        marginTop: '4px',
      }}>
        <span style={{ color: CHART_THEME.text, fontWeight: 500, fontSize: '12px' }}>
          Total
        </span>
        <span style={{ fontWeight: 700, color: CHART_THEME.text }}>
          {formatCurrency(data.total)}
        </span>
      </div>
    </div>
  )
}

// ─── Custom Legend ────────────────────────────────────────────────────────────

function CustomLegend() {
  return (
    <div style={{
      display: 'flex',
      justifyContent: 'center',
      gap: '20px',
      paddingTop: '8px',
      fontSize: '12px',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
        <div style={{
          width: '14px',
          height: '14px',
          borderRadius: '3px',
          background: PAID_COLOR,
        }} />
        <span style={{ color: CHART_THEME.text, fontWeight: 500 }}>Paid</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
        <div style={{
          width: '14px',
          height: '14px',
          borderRadius: '3px',
          background: ORGANIC_COLOR,
        }} />
        <span style={{ color: CHART_THEME.text, fontWeight: 500 }}>Organic</span>
      </div>
      <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
        <div style={{
          width: '14px',
          height: '14px',
          borderRadius: '3px',
          background: OTHER_COLOR,
        }} />
        <span style={{ color: CHART_THEME.text, fontWeight: 500 }}>Other</span>
      </div>
    </div>
  )
}

// ─── Component ────────────────────────────────────────────────────────────────

export const TrafficTrendChart = memo(function TrafficTrendChart() {
  const { data, isLoading, error, refetch } = useTrafficTrend()

  const chartData = useMemo(() => {
    if (!data?.trend?.length) return []

    return data.trend.map(day => {
      // Parse date to get short format (dd.mm)
      const parts = day.date.split('-') // "2026-02-01"
      const shortDate = parts.length === 3 ? `${parts[2]}.${parts[1]}` : day.date

      return {
        date: day.date,
        shortDate,
        paidRevenue: day.paid_revenue,
        organicRevenue: day.organic_revenue,
        otherRevenue: day.other_revenue,
        paidOrders: day.paid_orders,
        organicOrders: day.organic_orders,
        otherOrders: day.other_orders,
        total: day.paid_revenue + day.organic_revenue + day.other_revenue,
      }
    })
  }, [data])

  const isEmpty = !isLoading && chartData.length === 0

  // Calculate totals for header
  const totals = useMemo(() => {
    if (!chartData.length) return { paid: 0, organic: 0, other: 0 }
    return {
      paid: chartData.reduce((sum, d) => sum + d.paidRevenue, 0),
      organic: chartData.reduce((sum, d) => sum + d.organicRevenue, 0),
      other: chartData.reduce((sum, d) => sum + d.otherRevenue, 0),
    }
  }, [chartData])

  return (
    <ChartContainer
      title="Revenue Trend by Traffic Type"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xl"
      ariaLabel="Stacked bar chart showing daily paid vs organic revenue"
      action={
        !isLoading && totals.paid + totals.organic + totals.other > 0 ? (
          <div className="flex items-center gap-1.5 text-[10px] sm:text-xs flex-wrap">
            <span className="px-1.5 sm:px-2 py-0.5 sm:py-1 rounded-full bg-blue-50 text-blue-600 font-medium">
              Paid: {formatCurrency(totals.paid)}
            </span>
            <span className="px-1.5 sm:px-2 py-0.5 sm:py-1 rounded-full bg-emerald-50 text-emerald-600 font-medium">
              Organic: {formatCurrency(totals.organic)}
            </span>
            <span className="px-1.5 sm:px-2 py-0.5 sm:py-1 rounded-full bg-slate-100 text-slate-600 font-medium">
              Other: {formatCurrency(totals.other)}
            </span>
          </div>
        ) : undefined
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
              <CartesianGrid {...GRID_PROPS} vertical={false} />

              <XAxis
                dataKey="shortDate"
                {...X_AXIS_PROPS}
                interval={chartData.length > 14 ? Math.floor(chartData.length / 7) : 0}
                angle={chartData.length > 20 ? -45 : 0}
                textAnchor={chartData.length > 20 ? 'end' : 'middle'}
                height={chartData.length > 20 ? 50 : 30}
              />

              <YAxis hide />

              <Tooltip
                content={<CustomTooltip />}
                cursor={{ fill: 'rgba(0, 0, 0, 0.04)' }}
              />

              {/* Stacked bars - order: Other (bottom), Organic (middle), Paid (top) */}
              <Bar
                dataKey="otherRevenue"
                name="Other"
                stackId="revenue"
                fill={OTHER_COLOR}
                radius={[0, 0, 0, 0]}
                maxBarSize={50}
              />
              <Bar
                dataKey="organicRevenue"
                name="Organic"
                stackId="revenue"
                fill={ORGANIC_COLOR}
                radius={[0, 0, 0, 0]}
                maxBarSize={50}
              />
              <Bar
                dataKey="paidRevenue"
                name="Paid"
                stackId="revenue"
                fill={PAID_COLOR}
                radius={[4, 4, 0, 0]}
                maxBarSize={50}
              />

              <Legend content={() => null} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        <CustomLegend />
      </div>
    </ChartContainer>
  )
})
