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
  CHART_THEME,
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
  sharePercent: number
  [key: string]: string | number
}

// ─── Custom Tooltip ──────────────────────────────────────────────────────────

interface TooltipProps {
  active?: boolean
  payload?: Array<{
    payload: ChartDataPoint
  }>
}

function CustomTooltip({ active, payload }: TooltipProps) {
  if (!active || !payload?.length) return null

  const data = payload[0]?.payload
  if (!data) return null

  return (
    <div
      style={{
        ...TOOLTIP_STYLE,
        padding: '12px 16px',
        minWidth: '180px',
      }}
    >
      {/* Source name with color indicator */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        marginBottom: '8px',
        paddingBottom: '8px',
        borderBottom: `1px solid ${CHART_THEME.border}`,
      }}>
        <div style={{
          width: '12px',
          height: '12px',
          borderRadius: '50%',
          background: data.color,
          flexShrink: 0,
        }} />
        <span style={{
          fontWeight: 600,
          color: CHART_THEME.text,
          fontSize: '13px',
        }}>
          {data.name}
        </span>
      </div>

      {/* Revenue amount */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '4px',
      }}>
        <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>Revenue</span>
        <span style={{ fontWeight: 600, color: CHART_THEME.text, fontSize: '13px' }}>
          {formatCurrency(data.revenue)}
        </span>
      </div>

      {/* Percentage */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}>
        <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>Share</span>
        <span style={{
          fontWeight: 600,
          color: data.color,
          fontSize: '13px',
        }}>
          {data.sharePercent.toFixed(1)}%
        </span>
      </div>
    </div>
  )
}

// ─── Component ───────────────────────────────────────────────────────────────

export const RevenueBySourceChart = memo(function RevenueBySourceChart() {
  const { data, isLoading, error, refetch } = useSalesBySource()

  const { chartData, totalRevenue } = useMemo(() => {
    if (!data?.labels?.length) return { chartData: [], totalRevenue: 0 }

    // Calculate total first
    const total = data.revenue?.reduce((sum, val) => sum + (val ?? 0), 0) ?? 0

    const processed = data.labels.map((label, index) => {
      const revenue = data.revenue?.[index] ?? 0
      const sharePercent = total > 0 ? (revenue / total) * 100 : 0
      return {
        name: label,
        revenue,
        color: data.backgroundColor?.[index] ?? SOURCE_COLORS[index % 3] ?? '#2563EB',
        sharePercent,
      }
    })

    return { chartData: processed, totalRevenue: total }
  }, [data])

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
            <Tooltip content={<CustomTooltip />} />
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
