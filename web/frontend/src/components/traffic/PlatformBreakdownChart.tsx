import { memo, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import {
  PieChart,
  Pie,
  Cell,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  LabelList,
} from 'recharts'
import { ChartContainer } from '../charts/ChartContainer'
import { TOOLTIP_STYLE, Y_AXIS_PROPS, BAR_PROPS, CHART_THEME } from '../charts/config'
import { useTrafficAnalytics } from '../../hooks/useApi'
import { formatCurrency, formatNumber } from '../../utils/formatters'

// ─── Platform Colors ──────────────────────────────────────────────────────────

const PLATFORM_COLORS: Record<string, string> = {
  facebook: '#7C3AED',  // purple
  tiktok: '#06B6D4',    // cyan
  google: '#2563EB',    // blue
  instagram: '#EC4899', // pink
  email: '#10B981',     // green
  telegram: '#0EA5E9',  // sky blue
  manager: '#14B8A6',   // teal
  other: '#6B7280',     // gray
}

const getPlatformColor = (platform: string): string => {
  return PLATFORM_COLORS[platform.toLowerCase()] || PLATFORM_COLORS.other
}

const formatPlatformName = (platform: string): string => {
  const names: Record<string, string> = {
    facebook: 'Facebook',
    tiktok: 'TikTok',
    google: 'Google',
    instagram: 'Instagram',
    email: 'Email',
    telegram: 'Telegram',
    manager: 'Manager',
  }
  return names[platform.toLowerCase()] || platform.charAt(0).toUpperCase() + platform.slice(1)
}

// ─── Custom Tooltip ───────────────────────────────────────────────────────────

interface TooltipPayload {
  platform: string
  orders: number
  revenue: number
  pct?: number
}

function CustomTooltip({
  active,
  payload,
  showRevenue = true,
  t,
}: {
  active?: boolean
  payload?: Array<{ payload: TooltipPayload }>
  showRevenue?: boolean
  t: (key: string) => string
}) {
  if (!active || !payload?.length) return null

  const data = payload[0]?.payload
  if (!data) return null

  return (
    <div style={{ ...TOOLTIP_STYLE, minWidth: '140px' }}>
      <p style={{ fontWeight: 600, marginBottom: '6px', color: CHART_THEME.text }}>
        {formatPlatformName(data.platform)}
      </p>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
        <span style={{ color: CHART_THEME.muted }}>{t('summary.totalOrders')}:</span>
        <span style={{ fontWeight: 500 }}>{formatNumber(data.orders)}</span>
      </div>
      {showRevenue && (
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <span style={{ color: CHART_THEME.muted }}>{t('common.revenue')}:</span>
          <span style={{ fontWeight: 500 }}>{formatCurrency(data.revenue)}</span>
        </div>
      )}
      {data.pct !== undefined && (
        <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '4px', paddingTop: '4px', borderTop: `1px solid ${CHART_THEME.border}` }}>
          <span style={{ color: CHART_THEME.muted }}>{t('traffic.share')}</span>
          <span style={{ fontWeight: 600, color: getPlatformColor(data.platform) }}>{data.pct.toFixed(1)}%</span>
        </div>
      )}
    </div>
  )
}

// ─── Component ────────────────────────────────────────────────────────────────

export const PlatformBreakdownChart = memo(function PlatformBreakdownChart() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useTrafficAnalytics()

  const chartData = useMemo(() => {
    if (!data?.by_platform) return []

    const platforms = Object.entries(data.by_platform)
    if (platforms.length === 0) return []

    const totalRevenue = platforms.reduce((sum, [, p]) => sum + p.revenue, 0)

    return platforms
      .map(([platform, p]) => ({
        platform,
        name: formatPlatformName(platform),
        orders: p.orders,
        revenue: p.revenue,
        color: getPlatformColor(platform),
        pct: totalRevenue > 0 ? (p.revenue / totalRevenue) * 100 : 0,
      }))
      .sort((a, b) => b.revenue - a.revenue)
  }, [data])

  const isEmpty = !isLoading && chartData.length === 0

  return (
    <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
      {/* Revenue by Platform - Donut Chart */}
      <ChartContainer
        title={t('traffic.revenueByPlatform')}
        isLoading={isLoading}
        error={error as Error | null}
        onRetry={refetch}
        isEmpty={isEmpty}
        height="md"
        ariaLabel={t('traffic.revenueByPlatformDesc')}
      >
        <div style={{ height: 256 }}>
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
                  (percent ?? 0) > 0.05 ? `${name ?? ''}: ${((percent ?? 0) * 100).toFixed(0)}%` : ''
                }
                labelLine={false}
              >
                {chartData.map((entry, index) => (
                  <Cell key={`pie-${index}`} fill={entry.color} />
                ))}
              </Pie>
              <Tooltip content={<CustomTooltip showRevenue t={t} />} />
            </PieChart>
          </ResponsiveContainer>
        </div>

        {/* Legend */}
        <div className="mt-4 pt-4 border-t border-slate-200">
          <div className="flex flex-wrap justify-center gap-3">
            {chartData.slice(0, 6).map((entry) => (
              <div key={entry.platform} className="flex items-center gap-1.5">
                <div
                  className="w-3 h-3 rounded-sm"
                  style={{ backgroundColor: entry.color }}
                />
                <span className="text-xs text-slate-600">{entry.name}</span>
              </div>
            ))}
          </div>
        </div>
      </ChartContainer>

      {/* Orders by Platform - Horizontal Bar Chart */}
      <ChartContainer
        title={t('traffic.ordersByPlatform')}
        isLoading={isLoading}
        error={error as Error | null}
        onRetry={refetch}
        isEmpty={isEmpty}
        height="md"
        ariaLabel={t('traffic.ordersByPlatformDesc')}
      >
        <div style={{ height: 256 }}>
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={chartData}
              layout="vertical"
              margin={{ top: 5, right: 60, left: 80, bottom: 5 }}
            >
              <XAxis type="number" hide />
              <YAxis
                type="category"
                dataKey="name"
                {...Y_AXIS_PROPS}
                width={75}
                tick={{ fontSize: 12 }}
              />
              <Tooltip content={<CustomTooltip showRevenue={false} t={t} />} />
              <Bar
                dataKey="orders"
                {...BAR_PROPS}
                radius={[0, 4, 4, 0]}
              >
                {chartData.map((entry, index) => (
                  <Cell key={`bar-${index}`} fill={entry.color} />
                ))}
                <LabelList
                  dataKey="orders"
                  position="right"
                  formatter={(value: unknown) => formatNumber(value as number)}
                  style={{ fill: CHART_THEME.text, fontSize: 11, fontWeight: 500 }}
                />
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      </ChartContainer>
    </div>
  )
})
