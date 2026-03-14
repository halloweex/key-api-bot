import { useMemo, memo, useState } from 'react'
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
import { usePromocodeAnalytics } from '../../hooks'
import { formatCurrency, formatNumber, formatPercent } from '../../utils/formatters'
import { COLORS } from '../../utils/colors'

// ─── Types ───────────────────────────────────────────────────────────────────

interface RevenueDataPoint {
  name: string
  fullName: string
  revenue: number
  revenueLabel: string
}

interface OrdersDataPoint {
  name: string
  fullName: string
  orders: number
  ordersLabel: string
}

interface TableRow {
  promocode: string
  orders: number
  revenue: number
  uniqueCustomers: number
  aov: number
}

// ─── Label Formatters ─────────────────────────────────────────────────────────

const formatShortCurrency = (value: number): string => {
  if (value >= 1000000) return `₴${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `₴${(value / 1000).toFixed(0)}K`
  return `₴${value}`
}

// ─── Metric Card ─────────────────────────────────────────────────────────────

interface MetricCardProps {
  label: string
  value: string
  sub?: string
  colorClass: string
  bgClass: string
}

const MetricCard = memo(function MetricCard({ label, value, sub, colorClass, bgClass }: MetricCardProps) {
  return (
    <div className={`rounded-xl p-3 sm:p-4 border ${bgClass}`}>
      <p className="text-xs text-slate-600 font-medium">{label}</p>
      <p className={`text-lg sm:text-xl font-bold truncate ${colorClass}`}>{value}</p>
      {sub && <p className="text-[11px] text-slate-500 mt-0.5">{sub}</p>}
    </div>
  )
})

// ─── Sort Options ───────────────────────────────────────────────────────────

type SortKey = 'revenue' | 'orders' | 'aov' | 'uniqueCustomers'

// ─── Component ───────────────────────────────────────────────────────────────

export const PromocodeAnalyticsChart = memo(function PromocodeAnalyticsChart() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = usePromocodeAnalytics()
  const [sortBy, setSortBy] = useState<SortKey>('revenue')

  const revenueData = useMemo<RevenueDataPoint[]>(() => {
    if (!data?.topByRevenue?.labels?.length) return []
    return data.topByRevenue.labels.map((label, index) => {
      const revenue = data.topByRevenue.data?.[index] ?? 0
      return {
        name: truncateText(label, 18),
        fullName: label || 'Unknown',
        revenue,
        revenueLabel: formatShortCurrency(revenue),
      }
    })
  }, [data])

  const ordersData = useMemo<OrdersDataPoint[]>(() => {
    if (!data?.topByOrders?.labels?.length) return []
    return data.topByOrders.labels.map((label, index) => {
      const orders = data.topByOrders.data?.[index] ?? 0
      return {
        name: truncateText(label, 18),
        fullName: label || 'Unknown',
        orders,
        ordersLabel: String(orders),
      }
    })
  }, [data])

  const sortedTable = useMemo<TableRow[]>(() => {
    if (!data?.table?.length) return []
    return [...data.table].sort((a, b) => b[sortBy] - a[sortBy])
  }, [data, sortBy])

  const metrics = data?.metrics
  const isEmpty = !isLoading && revenueData.length === 0

  return (
    <ChartContainer
      title={t('chart.promocodeAnalytics')}
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xxl"
      ariaLabel={t('chart.promocodeAnalyticsDesc')}
    >
      {/* Metrics */}
      {metrics && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 sm:gap-3 mb-4">
          <MetricCard
            label={t('chart.promoTotalCodes')}
            value={formatNumber(metrics.totalCodes)}
            colorClass="text-purple-600"
            bgClass="bg-gradient-to-br from-purple-100 to-purple-50 border-purple-200"
          />
          <MetricCard
            label={t('chart.promoTopCode')}
            value={metrics.topCode}
            sub={`${formatPercent(metrics.topCodeShare)} ${t('chart.promoOfRevenue')}`}
            colorClass="text-blue-600"
            bgClass="bg-gradient-to-br from-blue-100 to-blue-50 border-blue-200"
          />
          <MetricCard
            label={t('chart.promoOrderShare')}
            value={formatPercent(metrics.promoOrderShare)}
            sub={`${formatNumber(metrics.promoOrders)} ${t('common.orders')}`}
            colorClass="text-green-600"
            bgClass="bg-gradient-to-br from-green-100 to-green-50 border-green-200"
          />
          <MetricCard
            label={t('chart.promoAov')}
            value={formatCurrency(metrics.promoAov)}
            sub={`${formatNumber(metrics.promoCustomers)} ${t('chart.promoCustomers')}`}
            colorClass="text-amber-600"
            bgClass="bg-gradient-to-br from-amber-100 to-amber-50 border-amber-200"
          />
        </div>
      )}

      {/* Charts: Revenue + Orders side by side */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 mb-4">
        {/* Top by Revenue */}
        <div>
          <h4 className="text-sm font-semibold text-slate-700 mb-2">{t('chart.promoTopByRevenue')}</h4>
          <div style={HEIGHT_STYLE.xxl}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={revenueData} layout="vertical" margin={{ left: 10, right: 55 }}>
                <CartesianGrid {...GRID_PROPS} horizontal={false} />
                <XAxis type="number" hide />
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
                <Bar dataKey="revenue" fill={COLORS.primary} {...BAR_PROPS}>
                  <LabelList dataKey="revenueLabel" position="right" style={LABEL_STYLE.default} />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Top by Orders */}
        <div>
          <h4 className="text-sm font-semibold text-slate-700 mb-2">{t('chart.promoTopByOrders')}</h4>
          <div style={HEIGHT_STYLE.xxl}>
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={ordersData} layout="vertical" margin={{ left: 10, right: 45 }}>
                <CartesianGrid {...GRID_PROPS} horizontal={false} />
                <XAxis type="number" hide />
                <YAxis
                  type="category"
                  dataKey="name"
                  {...Y_AXIS_PROPS}
                  fontSize={CHART_DIMENSIONS.fontSize.xs}
                  width={CHART_DIMENSIONS.yAxisWidth.md}
                />
                <Tooltip
                  contentStyle={TOOLTIP_STYLE}
                  formatter={(value) => [formatNumber(Number(value) || 0), t('common.orders')]}
                  labelFormatter={(_label, payload) => {
                    const item = payload?.[0]?.payload as OrdersDataPoint | undefined
                    return item?.fullName || String(_label)
                  }}
                />
                <Bar dataKey="orders" fill={CHART_THEME.accent} {...BAR_PROPS}>
                  <LabelList dataKey="ordersLabel" position="right" style={LABEL_STYLE.default} />
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Table */}
      {sortedTable.length > 0 && (
        <div>
          <div className="flex items-center justify-between mb-2">
            <h4 className="text-sm font-semibold text-slate-700">{t('chart.promoAllCodes')}</h4>
            <div className="flex items-center gap-1.5 text-xs text-slate-500">
              <span>{t('chart.promoSortBy')}:</span>
              {(['revenue', 'orders', 'aov', 'uniqueCustomers'] as SortKey[]).map((key) => (
                <button
                  key={key}
                  onClick={() => setSortBy(key)}
                  className={`px-2 py-0.5 rounded-md transition-colors ${
                    sortBy === key
                      ? 'bg-purple-100 text-purple-700 font-medium'
                      : 'hover:bg-slate-100'
                  }`}
                >
                  {t(`chart.promoSort_${key}`)}
                </button>
              ))}
            </div>
          </div>
          <div className="overflow-x-auto rounded-lg border border-slate-200">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-slate-50 text-slate-600 text-left">
                  <th className="px-3 py-2 font-medium">{t('chart.promoCode')}</th>
                  <th className="px-3 py-2 font-medium text-right">{t('common.orders')}</th>
                  <th className="px-3 py-2 font-medium text-right">{t('common.revenue')}</th>
                  <th className="px-3 py-2 font-medium text-right">{t('chart.promoCustomers')}</th>
                  <th className="px-3 py-2 font-medium text-right">{t('chart.promoAovShort')}</th>
                </tr>
              </thead>
              <tbody>
                {sortedTable.map((row) => (
                  <tr key={row.promocode} className="border-t border-slate-100 hover:bg-slate-50">
                    <td className="px-3 py-2 font-medium text-slate-800">{row.promocode}</td>
                    <td className="px-3 py-2 text-right text-slate-600">{formatNumber(row.orders)}</td>
                    <td className="px-3 py-2 text-right text-slate-600">{formatCurrency(row.revenue)}</td>
                    <td className="px-3 py-2 text-right text-slate-600">{formatNumber(row.uniqueCustomers)}</td>
                    <td className="px-3 py-2 text-right text-slate-600">{formatCurrency(row.aov)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </ChartContainer>
  )
})
