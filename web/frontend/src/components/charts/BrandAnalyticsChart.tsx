import { useMemo, memo } from 'react'
import { useTranslation } from 'react-i18next'
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
  TOOLTIP_STYLE,
  GRID_PROPS,
  Y_AXIS_PROPS,
  BAR_PROPS,
  LABEL_STYLE,
  HEIGHT_STYLE,
  CHART_DIMENSIONS,
  truncateText,
} from './config'
import { useBrandAnalytics } from '../../hooks'
import { MetricCard } from '../MetricCard'
import { formatCurrency, formatNumber, formatPercent } from '../../utils/formatters'
import { COLORS } from '../../utils/colors'
import { TagIcon, TrophyIcon, ChartPieIcon } from '../icons'

// ─── Types ───────────────────────────────────────────────────────────────────

interface RevenueDataPoint {
  name: string
  fullName: string
  revenue: number
  revenueLabel: string
}

interface QuantityDataPoint {
  name: string
  fullName: string
  quantity: number
  quantityLabel: string
}

// ─── Label Formatters ─────────────────────────────────────────────────────────

const formatShortCurrency = (value: number): string => {
  if (value >= 1000000) {
    return `₴${(value / 1000000).toFixed(1)}M`
  }
  if (value >= 1000) {
    return `₴${(value / 1000).toFixed(0)}K`
  }
  return `₴${value}`
}

const formatShortNumber = (value: number): string => {
  if (value >= 1000) {
    return `${(value / 1000).toFixed(1)}K`
  }
  return String(value)
}

// ─── Component ───────────────────────────────────────────────────────────────

export const BrandAnalyticsChart = memo(function BrandAnalyticsChart() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useBrandAnalytics()

  const revenueData = useMemo<RevenueDataPoint[]>(() => {
    if (!data?.topByRevenue?.labels?.length) return []
    return data.topByRevenue.labels.map((label, index) => {
      const revenue = data.topByRevenue.data?.[index] ?? 0
      return {
        name: truncateText(label, 15),
        fullName: label || 'Unknown',
        revenue,
        revenueLabel: formatShortCurrency(revenue),
      }
    })
  }, [data])

  const quantityData = useMemo<QuantityDataPoint[]>(() => {
    if (!data?.topByQuantity?.labels?.length) return []
    return data.topByQuantity.labels.map((label, index) => {
      const quantity = data.topByQuantity.data?.[index] ?? 0
      return {
        name: truncateText(label, 15),
        fullName: label || 'Unknown',
        quantity,
        quantityLabel: formatShortNumber(quantity),
      }
    })
  }, [data])

  const metrics = data?.metrics
  const isEmpty = !isLoading && revenueData.length === 0

  return (
    <ChartContainer
      title={t('chart.brandAnalytics')}
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xxl"
      ariaLabel={t('chart.brandAnalyticsDesc')}
    >
      {/* Metrics */}
      {metrics && (
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 sm:gap-3 mb-4">
          <MetricCard
            surface="tile-gradient"
            tone="blue"
            icon={<TagIcon />}
            label={t('chart.totalBrands')}
            value={formatNumber(metrics.totalBrands ?? 0)}
          />
          <MetricCard
            surface="tile-gradient"
            tone="purple"
            icon={<TrophyIcon />}
            label={t('chart.topBrand')}
            value={metrics.topBrand ?? t('chart.na')}
          />
          <MetricCard
            surface="tile-gradient"
            tone="green"
            icon={<ChartPieIcon />}
            label={t('chart.topBrandShare')}
            value={formatPercent(metrics.topBrandShare ?? 0)}
          />
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top Brands by Revenue */}
        <div>
          <h4 className="text-sm font-semibold text-slate-700 mb-2">{t('chart.top10ByRevenue')}</h4>
          <div style={HEIGHT_STYLE.xxl}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={revenueData}
                layout="vertical"
                margin={{ left: 10, right: 50 }}
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
                  fontSize={CHART_DIMENSIONS.fontSize.xs}
                  width={CHART_DIMENSIONS.yAxisWidth.md}
                />
                <Tooltip
                  contentStyle={TOOLTIP_STYLE}
                  formatter={(value) => [formatCurrency(Number(value) || 0), t('chart.revenue')]}
                  labelFormatter={(_label, payload) => {
                    const item = payload?.[0]?.payload as RevenueDataPoint | undefined
                    return item?.fullName || String(_label)
                  }}
                />
                <Bar
                  dataKey="revenue"
                  fill={COLORS.primary}
                  {...BAR_PROPS}
                >
                  <LabelList
                    dataKey="revenueLabel"
                    position="right"
                    style={LABEL_STYLE.default}
                  />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Top Brands by Quantity */}
        <div>
          <h4 className="text-sm font-semibold text-slate-700 mb-2">{t('chart.top10ByQuantity')}</h4>
          <div style={HEIGHT_STYLE.xxl}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={quantityData}
                layout="vertical"
                margin={{ left: 10, right: 45 }}
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
                  fontSize={CHART_DIMENSIONS.fontSize.xs}
                  width={CHART_DIMENSIONS.yAxisWidth.md}
                />
                <Tooltip
                  contentStyle={TOOLTIP_STYLE}
                  formatter={(value) => [formatNumber(Number(value) || 0), t('chart.quantity')]}
                  labelFormatter={(_label, payload) => {
                    const item = payload?.[0]?.payload as QuantityDataPoint | undefined
                    return item?.fullName || String(_label)
                  }}
                />
                <Bar
                  dataKey="quantity"
                  fill={CHART_THEME.accent}
                  {...BAR_PROPS}
                >
                  <LabelList
                    dataKey="quantityLabel"
                    position="right"
                    style={LABEL_STYLE.default}
                  />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </ChartContainer>
  )
})
