import { useMemo, memo, useState } from 'react'
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

// ─── Icons ────────────────────────────────────────────────────────────────────

const UserPlusIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M18 9v3m0 0v3m0-3h3m-3 0h-3m-2-5a4 4 0 11-8 0 4 4 0 018 0zM3 20a6 6 0 0112 0v1H3v-1z" />
  </svg>
)

const UserGroupIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
  </svg>
)

const RefreshIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
  </svg>
)

const CurrencyIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
  </svg>
)

const HeartIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M4.318 6.318a4.5 4.5 0 000 6.364L12 20.364l7.682-7.682a4.5 4.5 0 00-6.364-6.364L12 7.636l-1.318-1.318a4.5 4.5 0 00-6.364 0z" />
  </svg>
)

const ShoppingBagIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M16 11V7a4 4 0 00-8 0v4M5 9h14l1 12H4L5 9z" />
  </svg>
)

const CalendarIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
  </svg>
)

// ─── Info Button ──────────────────────────────────────────────────────────────

function InfoButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className="text-slate-400 hover:text-slate-600 transition-colors"
      aria-label="How is this calculated?"
    >
      <svg className="w-4 h-4" viewBox="0 0 24 24" fill="currentColor">
        <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 17h-2v-2h2v2zm2.07-7.75l-.9.92C13.45 12.9 13 13.5 13 15h-2v-.5c0-1.1.45-2.1 1.17-2.83l1.24-1.26c.37-.36.59-.86.59-1.41 0-1.1-.9-2-2-2s-2 .9-2 2H8c0-2.21 1.79-4 4-4s4 1.79 4 4c0 .88-.36 1.68-.93 2.25z"/>
      </svg>
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
      title="Customer Lifetime Value Metrics"
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
              label="New Customers"
              value={formatNumber(metrics.newCustomers ?? 0)}
              subtitle={`${newPercent}% of total`}
              colorClass="text-blue-600"
              bgClass="bg-gradient-to-br from-blue-100 to-blue-50 border border-blue-200"
            />
            <SummaryCard
              icon={<UserGroupIcon />}
              label="Returning Customers"
              value={formatNumber(metrics.returningCustomers ?? 0)}
              subtitle={`${returningPercent}% of total`}
              colorClass="text-purple-600"
              bgClass="bg-gradient-to-br from-purple-100 to-purple-50 border border-purple-200"
            />
            <SummaryCard
              icon={<RefreshIcon />}
              label="Repeat Rate"
              value={formatPercent(metrics.repeatRate ?? 0)}
              subtitle="Orders from returning"
              colorClass="text-green-600"
              bgClass="bg-gradient-to-br from-green-100 to-green-50 border border-green-200"
            />
            <SummaryCard
              icon={<CurrencyIcon />}
              label="Avg Order Value"
              value={formatCurrency(metrics.averageOrderValue ?? 0)}
              subtitle="Per order"
              colorClass="text-orange-600"
              bgClass="bg-gradient-to-br from-orange-100 to-orange-50 border border-orange-200"
            />
          </div>

          {/* Row 2: Repeat Customer Behavior */}
          {metrics.customerLifetimeValue !== undefined && (
            <div className="relative">
              <div className="flex items-center gap-1.5 mb-2">
                <h4 className="text-xs font-medium text-slate-500 uppercase tracking-wide">
                  Repeat Customer Behavior
                </h4>
                <InfoButton onClick={() => setShowClvInfo(!showClvInfo)} />
                {showClvInfo && (
                  <InfoTooltipContent onClose={() => setShowClvInfo(false)} title="Repeat Customer Metrics">
                    <p className="text-xs text-slate-300 mb-2">
                      <strong className="text-rose-400">CLV:</strong> Average total revenue per repeat customer.
                    </p>
                    <p className="text-xs text-slate-300 mb-2">
                      <strong className="text-indigo-400">Purchase Frequency:</strong> Avg orders per repeat customer.
                    </p>
                    <p className="text-xs text-slate-300">
                      <strong className="text-teal-400">Lifespan:</strong> Avg days between first and last order.
                    </p>
                  </InfoTooltipContent>
                )}
              </div>
              <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
                <SummaryCard
                  icon={<HeartIcon />}
                  label="Customer Lifetime Value"
                  value={formatCurrency(metrics.customerLifetimeValue ?? 0)}
                  subtitle="Avg revenue per repeat customer"
                  colorClass="text-rose-600"
                  bgClass="bg-gradient-to-br from-rose-100 to-rose-50 border border-rose-200"
                />
                <SummaryCard
                  icon={<ShoppingBagIcon />}
                  label="Purchase Frequency"
                  value={`${(metrics.avgPurchaseFrequency ?? 0).toFixed(1)}x`}
                  subtitle="Avg orders per repeat customer"
                  colorClass="text-indigo-600"
                  bgClass="bg-gradient-to-br from-indigo-100 to-indigo-50 border border-indigo-200"
                />
                <SummaryCard
                  icon={<CalendarIcon />}
                  label="Customer Lifespan"
                  value={`${Math.round(metrics.avgCustomerLifespanDays ?? 0)} days`}
                  subtitle="Avg time from first to last order"
                  colorClass="text-teal-600"
                  bgClass="bg-gradient-to-br from-teal-100 to-teal-50 border border-teal-200"
                />
                <SummaryCard
                  icon={<RefreshIcon />}
                  label="Orders per Customer"
                  value={`${(metrics.purchaseFrequency ?? 0).toFixed(2)}x`}
                  subtitle="In selected period"
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
                  All-Time
                </h4>
              </div>
              <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
                <SummaryCard
                  icon={<UserGroupIcon />}
                  label="Total Customers"
                  value={formatNumber(metrics.totalCustomersAllTime ?? 0)}
                  subtitle="Unique customers"
                  colorClass="text-cyan-600"
                  bgClass="bg-gradient-to-br from-cyan-100 to-cyan-50 border border-cyan-200"
                />
                <SummaryCard
                  icon={<RefreshIcon />}
                  label="True Repeat Rate"
                  value={formatPercent(metrics.trueRepeatRate ?? 0)}
                  subtitle={`${formatNumber(metrics.repeatCustomersAllTime ?? 0)} repeat customers`}
                  colorClass="text-green-600"
                  bgClass="bg-gradient-to-br from-green-100 to-green-50 border border-green-200"
                />
                <SummaryCard
                  icon={<ShoppingBagIcon />}
                  label="Orders per Customer"
                  value={`${(metrics.avgOrdersPerCustomer ?? 0).toFixed(2)}x`}
                  subtitle="Average"
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
              Customers in Period (New vs Returning)
              <InfoButton onClick={() => setShowCustomerInfo(!showCustomerInfo)} />
            </h4>
            {showCustomerInfo && (
              <InfoTooltipContent onClose={() => setShowCustomerInfo(false)} title="Period-Based Customer Split">
                <p className="text-xs text-slate-300 mb-2">
                  <strong className="text-blue-400">New:</strong> First order was in selected period.
                </p>
                <p className="text-xs text-slate-300 mb-2">
                  <strong className="text-purple-400">Returning:</strong> Had orders before selected period.
                </p>
                <p className="text-xs text-slate-300">
                  <strong className="text-slate-400">Note:</strong> Shows customers who ordered in this period only.
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
                  formatter={(value) => [formatNumber(Number(value) || 0), 'Customers']}
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
              Average Order Value Trend
              <InfoButton onClick={() => setShowAovInfo(!showAovInfo)} />
            </h4>
            {showAovInfo && (
              <InfoTooltipContent onClose={() => setShowAovInfo(false)} title="How is AOV calculated?">
                <p className="text-xs text-slate-300 mb-2">
                  <strong className="text-orange-400">AOV:</strong> Total Revenue ÷ Number of Orders per day.
                </p>
                <p className="text-xs text-slate-300 mb-2">
                  <strong className="text-blue-400">Revenue:</strong> Sum of product prices × quantities.
                </p>
                <p className="text-xs text-slate-300">
                  <strong className="text-slate-400">Excluded:</strong> Returns and cancelled orders.
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
                  formatter={(value) => [formatCurrency(Number(value) || 0), 'AOV']}
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
                Period Average: <span className="text-orange-600 font-semibold">
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
