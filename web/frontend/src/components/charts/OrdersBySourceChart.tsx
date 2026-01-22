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
import {
  CHART_THEME,
  CHART_DIMENSIONS,
  TOOLTIP_STYLE,
  GRID_PROPS,
  Y_AXIS_PROPS,
  BAR_PROPS,
} from './config'
import { useSalesBySource } from '../../hooks'
import { formatNumber } from '../../utils/formatters'
import { SOURCE_COLORS } from '../../utils/colors'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  name: string
  orders: number
  color: string
  label: string
  percent: number
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
        minWidth: '160px',
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
          borderRadius: '3px',
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

      {/* Orders count */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '4px',
      }}>
        <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>Orders</span>
        <span style={{ fontWeight: 600, color: CHART_THEME.text, fontSize: '13px' }}>
          {formatNumber(data.orders)}
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
          {data.percent.toFixed(1)}%
        </span>
      </div>
    </div>
  )
}

// ─── Component ───────────────────────────────────────────────────────────────

export const OrdersBySourceChart = memo(function OrdersBySourceChart() {
  const { data, isLoading, error, refetch } = useSalesBySource()

  const { chartData, totalOrders } = useMemo(() => {
    if (!data?.labels?.length) return { chartData: [], totalOrders: 0 }

    // Calculate total first
    const total = data.orders?.reduce((sum, val) => sum + (val ?? 0), 0) ?? 0

    const processed = data.labels.map((label, index) => {
      const orders = data.orders?.[index] ?? 0
      const percent = total > 0 ? (orders / total) * 100 : 0
      return {
        name: label,
        orders,
        color: data.backgroundColor?.[index] ?? SOURCE_COLORS[index % 3] ?? '#2563EB',
        label: `${formatNumber(orders)} (${percent.toFixed(0)}%)`,
        percent,
      }
    })

    return { chartData: processed, totalOrders: total }
  }, [data])

  const isEmpty = !isLoading && chartData.length === 0

  return (
    <ChartContainer
      title="Orders by Source"
      isLoading={isLoading}
      error={error as Error | null}
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
              content={<CustomTooltip />}
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
