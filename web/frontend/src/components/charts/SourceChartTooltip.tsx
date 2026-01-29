import { CHART_THEME, TOOLTIP_STYLE } from './config'
import { formatCurrency, formatNumber } from '../../utils/formatters'

// ─── Types ───────────────────────────────────────────────────────────────────

interface DataPoint {
  name: string
  revenue?: number
  orders?: number
  color: string
  revenuePercent?: number
  ordersPercent?: number
  sharePercent?: number
  percent?: number
}

interface TooltipPayload {
  payload: DataPoint
}

export interface SourceChartTooltipProps {
  active?: boolean
  payload?: TooltipPayload[]
  showRevenue?: boolean
  showOrders?: boolean
  indicatorShape?: 'square' | 'circle'
}

// ─── Component ───────────────────────────────────────────────────────────────

/**
 * Shared tooltip component for source charts.
 * Provides consistent styling across SalesBySource, OrdersBySource, and RevenueBySource charts.
 */
export function SourceChartTooltip({
  active,
  payload,
  showRevenue = true,
  showOrders = false,
  indicatorShape = 'square',
}: SourceChartTooltipProps) {
  if (!active || !payload?.length) return null

  const data = payload[0]?.payload
  if (!data) return null

  // Calculate percentage - handle different field names
  const percentage = data.revenuePercent ?? data.ordersPercent ?? data.sharePercent ?? data.percent ?? 0

  return (
    <div
      style={{
        ...TOOLTIP_STYLE,
        padding: '12px 16px',
        minWidth: showRevenue && showOrders ? '180px' : '160px',
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
          borderRadius: indicatorShape === 'circle' ? '50%' : '3px',
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

      {/* Revenue row */}
      {showRevenue && data.revenue !== undefined && (
        <div style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: showOrders ? '4px' : '4px',
        }}>
          <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>Revenue</span>
          <span style={{ fontWeight: 600, color: CHART_THEME.text, fontSize: '13px' }}>
            {formatCurrency(data.revenue)}
          </span>
        </div>
      )}

      {/* Orders row */}
      {showOrders && data.orders !== undefined && (
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
      )}

      {/* Percentage row */}
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
          {percentage.toFixed(1)}%
        </span>
      </div>
    </div>
  )
}
