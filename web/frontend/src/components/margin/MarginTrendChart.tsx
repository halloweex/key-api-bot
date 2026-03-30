import { useMemo, memo } from 'react'
import { useTranslation } from 'react-i18next'
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { ChartContainer } from '../charts/ChartContainer'
import {
  CHART_THEME,
  TOOLTIP_STYLE,
  GRID_PROPS,
  X_AXIS_PROPS,
  Y_AXIS_PROPS,
  LINE_PROPS,
  HEIGHT_STYLE,
  formatAxisK,
} from '../charts/config'
import { useMarginTrend } from '../../hooks'
import { formatCurrency, formatPercent } from '../../utils/formatters'
import { COLORS } from '../../utils/colors'

// ─── Component ───────────────────────────────────────────────────────────────

export const MarginTrendChart = memo(function MarginTrendChart() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useMarginTrend()

  const chartData = useMemo(() => {
    if (!data?.length) return []
    return data.map((item) => ({
      month: item.month,
      revenue: item.revenue,
      cogs: item.cogs,
      profit: item.profit,
      margin_pct: item.margin_pct,
    }))
  }, [data])

  const isEmpty = !isLoading && chartData.length === 0

  return (
    <ChartContainer
      title={t('margin.trend')}
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xl"
      ariaLabel={t('margin.trendDesc')}
    >
      <div style={HEIGHT_STYLE.xl}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={chartData} margin={{ top: 5, right: 30, left: 10, bottom: 5 }}>
            <CartesianGrid {...GRID_PROPS} />
            <XAxis
              dataKey="month"
              {...X_AXIS_PROPS}
            />
            <YAxis
              yAxisId="left"
              {...Y_AXIS_PROPS}
              tickFormatter={formatAxisK}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              {...Y_AXIS_PROPS}
              tickFormatter={(v) => `${v}%`}
              domain={[0, 100]}
            />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              formatter={(value, name) => {
                const v = Number(value) || 0
                if (name === 'margin_pct') return [formatPercent(v), t('margin.margin')]
                if (name === 'revenue') return [formatCurrency(v), t('margin.costedRevenue')]
                if (name === 'cogs') return [formatCurrency(v), t('margin.cogs')]
                return [formatCurrency(v), t('margin.grossProfit')]
              }}
            />
            <Legend
              wrapperStyle={{ color: CHART_THEME.axis }}
              formatter={(value: string) => {
                const labels: Record<string, string> = {
                  revenue: t('margin.costedRevenue'),
                  cogs: t('margin.cogs'),
                  profit: t('margin.grossProfit'),
                  margin_pct: t('margin.marginPct'),
                }
                return labels[value] || value
              }}
            />
            <Bar
              yAxisId="left"
              dataKey="revenue"
              fill={COLORS.primary}
              radius={[4, 4, 0, 0]}
              opacity={0.3}
            />
            <Bar
              yAxisId="left"
              dataKey="cogs"
              fill={COLORS.warning}
              radius={[4, 4, 0, 0]}
              opacity={0.6}
            />
            <Bar
              yAxisId="left"
              dataKey="profit"
              fill={COLORS.success}
              radius={[4, 4, 0, 0]}
            />
            <Line
              yAxisId="right"
              dataKey="margin_pct"
              stroke={CHART_THEME.accent}
              {...LINE_PROPS}
              strokeWidth={3}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </ChartContainer>
  )
})
