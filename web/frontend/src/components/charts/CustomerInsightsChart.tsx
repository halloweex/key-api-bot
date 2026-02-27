import { useMemo, memo, useState } from 'react'
import { CircleHelp } from 'lucide-react'
import { useTranslation } from 'react-i18next'
import {
  PieChart,
  Pie,
  Cell,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import {
  TOOLTIP_STYLE,
  GRID_PROPS,
  X_AXIS_PROPS,
  Y_AXIS_PROPS,
  PIE_PROPS,
  formatAxisK,
} from './config'
import { useCustomerInsights } from '../../hooks'
import { formatCurrency, formatNumber, formatPercent } from '../../utils/formatters'
import { CUSTOMER_COLORS } from '../../utils/colors'
import {
  UserPlusIcon,
  UserGroupIcon,
  RefreshIcon,
  CurrencyIcon,
  HeartIcon,
  ShoppingBagIcon,
  CalendarIcon,
} from '../icons'

// ─── Types ───────────────────────────────────────────────────────────────────

interface PieDataPoint {
  name: string
  value: number
  color: string
  [key: string]: string | number
}

interface AOVDataPoint {
  date: string
  aov: number
}

// ─── Summary Card ─────────────────────────────────────────────────────────────

interface SummaryCardProps {
  icon: React.ReactNode
  label: string
  value: string
  subtitle?: string
  colorClass: string
  bgClass: string
}

const SummaryCard = memo(function SummaryCard({
  icon,
  label,
  value,
  subtitle,
  colorClass,
  bgClass
}: SummaryCardProps) {
  return (
    <div className={`rounded-xl p-4 ${bgClass}`}>
      <div className="flex items-start gap-3">
        <div className={`p-2 rounded-lg ${colorClass} bg-slate-800/10`}>
          {icon}
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-xs text-slate-600 font-medium">{label}</p>
          <p className={`text-xl font-bold ${colorClass}`}>{value}</p>
          {subtitle && (
            <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p>
          )}
        </div>
      </div>
    </div>
  )
})

// ─── Info Button ──────────────────────────────────────────────────────────────

function InfoButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className="text-slate-400 hover:text-slate-600 transition-colors"
      aria-label="How is this calculated?"
    >
      <CircleHelp className="w-4 h-4" />
    </button>
  )
}

// ─── Info Tooltip ─────────────────────────────────────────────────────────────

function InfoTooltipContent({ onClose, title, children }: {
  onClose: () => void
  title: string
  children: React.ReactNode
}) {
  return (
    <div className="absolute top-8 left-0 z-50 bg-slate-800 border border-slate-700 rounded-lg shadow-xl p-4 min-w-[220px] max-w-[300px]">
      <button
        onClick={onClose}
        className="absolute top-2 right-2 text-slate-400 hover:text-slate-200 text-lg leading-none"
        aria-label="Close"
      >
        ×
      </button>
      <h4 className="text-sm font-semibold text-slate-200 mb-2">{title}</h4>
      {children}
    </div>
  )
}

// ─── Component ───────────────────────────────────────────────────────────────

export const CustomerInsightsChart = memo(function CustomerInsightsChart() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useCustomerInsights()
  const [showCustomerInfo, setShowCustomerInfo] = useState(false)
  const [showAovInfo, setShowAovInfo] = useState(false)
  const [showClvInfo, setShowClvInfo] = useState(false)

  const pieData = useMemo<PieDataPoint[]>(() => {
    if (!data?.newVsReturning?.labels?.length) return []
    return data.newVsReturning.labels.map((label, index) => ({
      name: label,
      value: data.newVsReturning.data?.[index] ?? 0,
      color: data.newVsReturning.backgroundColor?.[index]
        ?? (index === 0 ? CUSTOMER_COLORS.new : CUSTOMER_COLORS.returning),
    }))
  }, [data])

  const aovData = useMemo<AOVDataPoint[]>(() => {
    if (!data?.aovTrend?.labels?.length) return []
    const aovValues = data.aovTrend.datasets?.[0]?.data ?? []
    return data.aovTrend.labels.map((label, index) => ({
      date: label,
      aov: aovValues[index] ?? 0,
    }))
  }, [data])

  const metrics = data?.metrics
  const isEmpty = !isLoading && pieData.length === 0

  // Calculate percentages for summary
  const totalCustomers = (metrics?.newCustomers ?? 0) + (metrics?.returningCustomers ?? 0)
  const newPercent = totalCustomers > 0 ? ((metrics?.newCustomers ?? 0) / totalCustomers * 100).toFixed(0) : '0'
  const returningPercent = totalCustomers > 0 ? ((metrics?.returningCustomers ?? 0) / totalCustomers * 100).toFixed(0) : '0'

  return (
    <ChartContainer
      title={t('customer.lifetimeMetrics')}
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xl"
      ariaLabel="Charts showing customer segmentation and average order value trends"
    >
      {/* Summary Cards - Top */}
      {metrics && (
        <div className="space-y-3 mb-6">
          {/* Row 1: Customer metrics */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
            <SummaryCard
              icon={<UserPlusIcon />}
              label={t('customer.newCustomers')}
              value={formatNumber(metrics.newCustomers ?? 0)}
              subtitle={`${newPercent}% ${t('customer.ofTotal')}`}
              colorClass="text-blue-600"
              bgClass="bg-gradient-to-br from-blue-100 to-blue-50 border border-blue-200"
            />
            <SummaryCard
              icon={<UserGroupIcon />}
              label={t('customer.returningCustomers')}
              value={formatNumber(metrics.returningCustomers ?? 0)}
              subtitle={`${returningPercent}% ${t('customer.ofTotal')}`}
              colorClass="text-purple-600"
              bgClass="bg-gradient-to-br from-purple-100 to-purple-50 border border-purple-200"
            />
            <SummaryCard
              icon={<RefreshIcon />}
              label={t('customer.repeatRate')}
              value={formatPercent(metrics.repeatRate ?? 0)}
              subtitle={t('customer.ordersFromReturning')}
              colorClass="text-green-600"
              bgClass="bg-gradient-to-br from-green-100 to-green-50 border border-green-200"
            />
            <SummaryCard
              icon={<CurrencyIcon />}
              label={t('customer.avgOrderValue')}
              value={formatCurrency(metrics.averageOrderValue ?? 0)}
              subtitle={t('customer.perOrder')}
              colorClass="text-orange-600"
              bgClass="bg-gradient-to-br from-orange-100 to-orange-50 border border-orange-200"
            />
          </div>

          {/* Row 2: Repeat Customer Behavior */}
          {metrics.customerLifetimeValue !== undefined && (
            <div className="relative">
              <div className="flex items-center gap-1.5 mb-2">
                <h4 className="text-xs font-medium text-slate-500 uppercase tracking-wide">
                  {t('customer.repeatBehavior')}
                </h4>
                <InfoButton onClick={() => setShowClvInfo(!showClvInfo)} />
                {showClvInfo && (
                  <InfoTooltipContent onClose={() => setShowClvInfo(false)} title={t('customer.repeatBehavior')}>
                    <p className="text-xs text-slate-300 mb-2">
                      <strong className="text-rose-400">CLV:</strong> {t('customer.clvDesc').replace('CLV: ', '')}
                    </p>
                    <p className="text-xs text-slate-300 mb-2">
                      <strong className="text-indigo-400">{t('customer.purchaseFrequency')}:</strong> {t('customer.freqDesc').replace('Purchase Frequency: ', '')}
                    </p>
                    <p className="text-xs text-slate-300">
                      <strong className="text-teal-400">{t('customer.customerLifespan')}:</strong> {t('customer.lifespanDesc').replace('Lifespan: ', '')}
                    </p>
                  </InfoTooltipContent>
                )}
              </div>
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
                <SummaryCard
                  icon={<HeartIcon />}
                  label={t('customer.clv')}
                  value={formatCurrency(metrics.customerLifetimeValue ?? 0)}
                  subtitle={t('customer.clvShort')}
                  colorClass="text-rose-600"
                  bgClass="bg-gradient-to-br from-rose-100 to-rose-50 border border-rose-200"
                />
                <SummaryCard
                  icon={<ShoppingBagIcon />}
                  label={t('customer.purchaseFrequency')}
                  value={`${(metrics.avgPurchaseFrequency ?? 0).toFixed(1)}x`}
                  subtitle={t('customer.purchaseFrequencyShort')}
                  colorClass="text-indigo-600"
                  bgClass="bg-gradient-to-br from-indigo-100 to-indigo-50 border border-indigo-200"
                />
                <SummaryCard
                  icon={<CalendarIcon />}
                  label={t('customer.customerLifespan')}
                  value={`${Math.round(metrics.avgCustomerLifespanDays ?? 0)} ${t('customer.daysUnit')}`}
                  subtitle={t('customer.customerLifespanShort')}
                  colorClass="text-teal-600"
                  bgClass="bg-gradient-to-br from-teal-100 to-teal-50 border border-teal-200"
                />
                <SummaryCard
                  icon={<RefreshIcon />}
                  label={t('customer.ordersPerCustomer')}
                  value={`${(metrics.purchaseFrequency ?? 0).toFixed(2)}x`}
                  subtitle={t('customer.inSelectedPeriod')}
                  colorClass="text-amber-600"
                  bgClass="bg-gradient-to-br from-amber-100 to-amber-50 border border-amber-200"
                />
              </div>
            </div>
          )}

          {/* Row 3: All-Time Metrics */}
          {metrics.totalCustomersAllTime !== undefined && (
            <div className="relative">
              <div className="flex items-center gap-1.5 mb-2">
                <h4 className="text-xs font-medium text-slate-500 uppercase tracking-wide">
                  {t('customer.allTime')}
                </h4>
              </div>
              <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
                <SummaryCard
                  icon={<UserGroupIcon />}
                  label={t('customer.totalCustomers')}
                  value={formatNumber(metrics.totalCustomersAllTime ?? 0)}
                  subtitle={t('customer.uniqueCustomers')}
                  colorClass="text-cyan-600"
                  bgClass="bg-gradient-to-br from-cyan-100 to-cyan-50 border border-cyan-200"
                />
                <SummaryCard
                  icon={<RefreshIcon />}
                  label={t('customer.trueRepeatRate')}
                  value={formatPercent(metrics.trueRepeatRate ?? 0)}
                  subtitle={`${formatNumber(metrics.repeatCustomersAllTime ?? 0)} ${t('customer.repeatCustomersCount')}`}
                  colorClass="text-green-600"
                  bgClass="bg-gradient-to-br from-green-100 to-green-50 border border-green-200"
                />
                <SummaryCard
                  icon={<ShoppingBagIcon />}
                  label={t('customer.ordersPerCustomer')}
                  value={`${(metrics.avgOrdersPerCustomer ?? 0).toFixed(2)}x`}
                  subtitle={t('customer.average')}
                  colorClass="text-amber-600"
                  bgClass="bg-gradient-to-br from-amber-100 to-amber-50 border border-amber-200"
                />
              </div>
            </div>
          )}
        </div>
      )}

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* New vs Returning Pie Chart */}
        <div>
          <div className="relative">
            <h4 className="text-sm font-semibold text-slate-700 mb-3 flex items-center gap-1.5">
              {t('customer.customersInPeriod')}
              <InfoButton onClick={() => setShowCustomerInfo(!showCustomerInfo)} />
            </h4>
            {showCustomerInfo && (
              <InfoTooltipContent onClose={() => setShowCustomerInfo(false)} title={t('customer.periodSplit')}>
                <p className="text-xs text-slate-300 mb-2">
                  {t('customer.newDesc')}
                </p>
                <p className="text-xs text-slate-300 mb-2">
                  {t('customer.returningDesc')}
                </p>
                <p className="text-xs text-slate-300">
                  {t('customer.splitNote')}
                </p>
              </InfoTooltipContent>
            )}
          </div>
          <div className="h-[160px] sm:h-[180px]">
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={pieData}
                  cx="50%"
                  cy="50%"
                  innerRadius={45}
                  outerRadius={75}
                  dataKey="value"
                  nameKey="name"
                  {...PIE_PROPS}
                >
                  {pieData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={TOOLTIP_STYLE}
                  formatter={(value) => [formatNumber(Number(value) || 0), t('customer.customersLabel')]}
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
          {/* Legend below chart */}
          <div className="flex justify-center gap-6 mt-3">
            {pieData.map((entry) => {
              const total = pieData.reduce((sum, e) => sum + e.value, 0)
              const percent = total > 0 ? ((entry.value / total) * 100).toFixed(0) : '0'
              return (
                <div key={entry.name} className="flex items-center gap-2">
                  <div
                    className="w-3 h-3 rounded-full"
                    style={{ backgroundColor: entry.color }}
                  />
                  <span className="text-sm text-slate-700">
                    {entry.name}: <span className="font-semibold">{formatNumber(entry.value)}</span>
                    <span className="text-slate-500 ml-1">({percent}%)</span>
                  </span>
                </div>
              )
            })}
          </div>
        </div>

        {/* AOV Trend Line Chart */}
        <div>
          <div className="relative">
            <h4 className="text-sm font-semibold text-slate-700 mb-3 flex items-center gap-1.5">
              {t('customer.aovTrend')}
              <InfoButton onClick={() => setShowAovInfo(!showAovInfo)} />
            </h4>
            {showAovInfo && (
              <InfoTooltipContent onClose={() => setShowAovInfo(false)} title={t('customer.aovCalcTitle')}>
                <p className="text-xs text-slate-300 mb-2">
                  {t('customer.aovCalcDesc')}
                </p>
                <p className="text-xs text-slate-300 mb-2">
                  {t('customer.aovRevenueDesc')}
                </p>
                <p className="text-xs text-slate-300">
                  {t('customer.aovExcluded')}
                </p>
              </InfoTooltipContent>
            )}
          </div>
          <div className="h-[180px] sm:h-[200px]">
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={aovData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
                <CartesianGrid {...GRID_PROPS} />
                <XAxis
                  dataKey="date"
                  {...X_AXIS_PROPS}
                  fontSize={10}
                  interval="preserveStartEnd"
                />
                <YAxis
                  {...Y_AXIS_PROPS}
                  fontSize={10}
                  tickFormatter={formatAxisK}
                  width={45}
                />
                <Tooltip
                  contentStyle={TOOLTIP_STYLE}
                  formatter={(value) => [formatCurrency(Number(value) || 0), t('customer.aovLabel')]}
                />
                <Line
                  type="monotone"
                  dataKey="aov"
                  stroke="#f97316"
                  strokeWidth={2}
                  dot={false}
                  activeDot={{ r: 4, fill: '#f97316', stroke: '#fff', strokeWidth: 2 }}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
          {/* Average indicator */}
          {aovData.length > 0 && (
            <div className="flex justify-center mt-2">
              <span className="text-sm text-slate-600">
                {t('customer.periodAverage')} <span className="text-orange-600 font-semibold">
                  {formatCurrency(aovData.reduce((sum, d) => sum + d.aov, 0) / aovData.length)}
                </span>
              </span>
            </div>
          )}
        </div>
      </div>
    </ChartContainer>
  )
})
