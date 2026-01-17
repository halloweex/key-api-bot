import { useMemo, memo } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import { Card, CardContent } from '../ui'
import {
  CHART_THEME,
  CHART_DIMENSIONS,
  TOOLTIP_STYLE,
  GRID_PROPS,
  X_AXIS_PROPS,
  Y_AXIS_PROPS,
  BAR_PROPS,
  formatAxisK,
  truncateText,
} from './config'
import { useBrandAnalytics } from '../../hooks'
import { formatCurrency, formatNumber, formatPercent } from '../../utils/formatters'
import { COLORS } from '../../utils/colors'

// ─── Types ───────────────────────────────────────────────────────────────────

interface RevenueDataPoint {
  name: string
  fullName: string
  revenue: number
}

interface QuantityDataPoint {
  name: string
  fullName: string
  quantity: number
}

// ─── Metric Card ─────────────────────────────────────────────────────────────

interface MetricCardProps {
  label: string
  value: string
  colorClass: string
}

const MetricCard = memo(function MetricCard({ label, value, colorClass }: MetricCardProps) {
  return (
    <Card className="bg-slate-700/50">
      <CardContent className="py-2 px-3">
        <p className="text-xs text-slate-400">{label}</p>
        <p className={`text-lg font-semibold truncate ${colorClass}`}>{value}</p>
      </CardContent>
    </Card>
  )
})

// ─── Component ───────────────────────────────────────────────────────────────

export const BrandAnalyticsChart = memo(function BrandAnalyticsChart() {
  const { data, isLoading, error, refetch } = useBrandAnalytics()

  const revenueData = useMemo<RevenueDataPoint[]>(() => {
    if (!data?.topByRevenue?.labels?.length) return []
    return data.topByRevenue.labels.map((label, index) => ({
      name: truncateText(label, 15),
      fullName: label || 'Unknown',
      revenue: data.topByRevenue.data?.[index] ?? 0,
    }))
  }, [data])

  const quantityData = useMemo<QuantityDataPoint[]>(() => {
    if (!data?.topByQuantity?.labels?.length) return []
    return data.topByQuantity.labels.map((label, index) => ({
      name: truncateText(label, 15),
      fullName: label || 'Unknown',
      quantity: data.topByQuantity.data?.[index] ?? 0,
    }))
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
        <div className="grid grid-cols-3 gap-3 mb-4">
          <MetricCard
            label="Total Brands"
            value={formatNumber(metrics.totalBrands ?? 0)}
            colorClass="text-blue-400"
          />
          <MetricCard
            label="Top Brand"
            value={metrics.topBrand ?? 'N/A'}
            colorClass="text-purple-400"
          />
          <MetricCard
            label="Top Brand Share"
            value={formatPercent(metrics.topBrandShare ?? 0)}
            colorClass="text-green-400"
          />
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top Brands by Revenue */}
        <div>
          <h4 className="text-sm font-medium text-slate-400 mb-2">Top 10 by Revenue</h4>
          <div style={{ height: CHART_DIMENSIONS.height.xxl }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={revenueData}
                layout="vertical"
                margin={{ left: 10, right: 20 }}
              >
                <CartesianGrid {...GRID_PROPS} horizontal={false} />
                <XAxis
                  type="number"
                  {...X_AXIS_PROPS}
                  fontSize={CHART_DIMENSIONS.fontSize.xs}
                  tickFormatter={formatAxisK}
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
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Top Brands by Quantity */}
        <div>
          <h4 className="text-sm font-medium text-slate-400 mb-2">Top 10 by Quantity</h4>
          <div style={{ height: CHART_DIMENSIONS.height.xxl }}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart
                data={quantityData}
                layout="vertical"
                margin={{ left: 10, right: 20 }}
              >
                <CartesianGrid {...GRID_PROPS} horizontal={false} />
                <XAxis
                  type="number"
                  {...X_AXIS_PROPS}
                  fontSize={CHART_DIMENSIONS.fontSize.xs}
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
                />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </ChartContainer>
  )
})
