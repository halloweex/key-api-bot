import { useMemo, memo } from 'react'
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

// ─── Metric Card ─────────────────────────────────────────────────────────────

interface MetricCardProps {
  icon: React.ReactNode
  label: string
  value: string
  colorClass: string
  bgClass: string
  iconBgClass: string
}

const MetricCard = memo(function MetricCard({ icon, label, value, colorClass, bgClass, iconBgClass }: MetricCardProps) {
  return (
    <div className={`rounded-xl p-4 border ${bgClass}`}>
      <div className="flex items-start gap-3">
        <div className={`p-2 lg:p-2.5 rounded-lg ${iconBgClass} ${colorClass}`}>
          {icon}
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-xs text-slate-600 font-medium">{label}</p>
          <p className={`text-xl font-bold truncate ${colorClass}`}>{value}</p>
        </div>
      </div>
    </div>
  )
})

// ─── Component ───────────────────────────────────────────────────────────────

export const BrandAnalyticsChart = memo(function BrandAnalyticsChart() {
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
      title="Brand Analytics"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xxl"
      ariaLabel="Bar charts showing top brands by revenue and quantity"
    >
      {/* Metrics */}
      {metrics && (
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-2 sm:gap-3 mb-4">
          <MetricCard
            icon={<TagIcon />}
            label="Total Brands"
            value={formatNumber(metrics.totalBrands ?? 0)}
            colorClass="text-blue-600"
            bgClass="bg-gradient-to-br from-blue-100 to-blue-50 border-blue-200"
            iconBgClass="bg-blue-200/60"
          />
          <MetricCard
            icon={<TrophyIcon />}
            label="Top Brand"
            value={metrics.topBrand ?? 'N/A'}
            colorClass="text-purple-600"
            bgClass="bg-gradient-to-br from-purple-100 to-purple-50 border-purple-200"
            iconBgClass="bg-purple-200/60"
          />
          <MetricCard
            icon={<ChartPieIcon />}
            label="Top Brand Share"
            value={formatPercent(metrics.topBrandShare ?? 0)}
            colorClass="text-green-600"
            bgClass="bg-gradient-to-br from-green-100 to-green-50 border-green-200"
            iconBgClass="bg-green-200/60"
          />
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top Brands by Revenue */}
        <div>
          <h4 className="text-sm font-semibold text-slate-700 mb-2">Top 10 by Revenue</h4>
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
                  formatter={(value) => [formatCurrency(Number(value) || 0), 'Revenue']}
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
          <h4 className="text-sm font-semibold text-slate-700 mb-2">Top 10 by Quantity</h4>
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
                  formatter={(value) => [formatNumber(Number(value) || 0), 'Quantity']}
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
