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
import { Card, CardContent } from '../ui'
import {
  CHART_DIMENSIONS,
  TOOLTIP_STYLE,
  GRID_PROPS,
  X_AXIS_PROPS,
  Y_AXIS_PROPS,
  LINE_PROPS,
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
  [key: string]: string | number  // Recharts compatibility
}

interface AOVDataPoint {
  date: string
  aov: number
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
        <p className={`text-lg font-semibold ${colorClass}`}>{value}</p>
      </CardContent>
    </Card>
  )
})

// ─── Info Button ──────────────────────────────────────────────────────────────

function InfoButton({ onClick }: { onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      className="ml-2 text-slate-400 hover:text-slate-300 transition-colors"
      aria-label="How is this calculated?"
    >
      <svg className="w-4 h-4" viewBox="0 0 24 24" fill="currentColor">
        <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm1 17h-2v-2h2v2zm2.07-7.75l-.9.92C13.45 12.9 13 13.5 13 15h-2v-.5c0-1.1.45-2.1 1.17-2.83l1.24-1.26c.37-.36.59-.86.59-1.41 0-1.1-.9-2-2-2s-2 .9-2 2H8c0-2.21 1.79-4 4-4s4 1.79 4 4c0 .88-.36 1.68-.93 2.25z"/>
      </svg>
    </button>
  )
}

// ─── Info Tooltips ────────────────────────────────────────────────────────────

function CustomerInfoTooltip({ onClose }: { onClose: () => void }) {
  return (
    <div className="absolute top-8 left-0 z-10 bg-slate-800 border border-slate-600 rounded-lg shadow-xl p-4 max-w-xs">
      <button
        onClick={onClose}
        className="absolute top-2 right-2 text-slate-400 hover:text-slate-200"
        aria-label="Close"
      >
        ×
      </button>
      <h4 className="text-sm font-semibold text-slate-200 mb-2">How is this calculated?</h4>
      <p className="text-xs text-slate-300 mb-2">
        <strong className="text-blue-400">New Customers:</strong> Customers whose account was created during the selected period.
      </p>
      <p className="text-xs text-slate-300 mb-2">
        <strong className="text-purple-400">Returning Customers:</strong> Customers whose account existed before the selected period started.
      </p>
      <p className="text-xs text-slate-300">
        <strong className="text-green-400">Repeat Rate:</strong> Percentage of orders from returning customers.
      </p>
    </div>
  )
}

function AOVInfoTooltip({ onClose }: { onClose: () => void }) {
  return (
    <div className="absolute top-8 left-0 z-10 bg-slate-800 border border-slate-600 rounded-lg shadow-xl p-4 max-w-xs">
      <button
        onClick={onClose}
        className="absolute top-2 right-2 text-slate-400 hover:text-slate-200"
        aria-label="Close"
      >
        ×
      </button>
      <h4 className="text-sm font-semibold text-slate-200 mb-2">How is AOV calculated?</h4>
      <p className="text-xs text-slate-300 mb-2">
        <strong className="text-orange-400">AOV (Average Order Value):</strong> Total Revenue ÷ Number of Orders for each day.
      </p>
      <p className="text-xs text-slate-300 mb-2">
        <strong className="text-blue-400">Revenue:</strong> Sum of product prices × quantities.
      </p>
      <p className="text-xs text-slate-300">
        <strong className="text-slate-400">Excluded:</strong> Orders with return/cancel statuses are not counted.
      </p>
    </div>
  )
}

// ─── Component ───────────────────────────────────────────────────────────────

export const CustomerInsightsChart = memo(function CustomerInsightsChart() {
  const { data, isLoading, error, refetch } = useCustomerInsights()
  const [showCustomerInfo, setShowCustomerInfo] = useState(false)
  const [showAovInfo, setShowAovInfo] = useState(false)

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
    // API returns Chart.js format: datasets[0].data
    const aovValues = data.aovTrend.datasets?.[0]?.data ?? []
    return data.aovTrend.labels.map((label, index) => ({
      date: label,
      aov: aovValues[index] ?? 0,
    }))
  }, [data])

  const metrics = data?.metrics
  const isEmpty = !isLoading && pieData.length === 0

  return (
    <ChartContainer
      title="Customer Insights"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="sm"
      ariaLabel="Charts showing customer segmentation and average order value trends"
    >
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* New vs Returning Pie Chart */}
        <div>
          <div className="relative">
            <h4 className="text-sm font-medium text-slate-400 mb-2 flex items-center">
              New vs Returning
              <InfoButton onClick={() => setShowCustomerInfo(!showCustomerInfo)} />
            </h4>
            {showCustomerInfo && <CustomerInfoTooltip onClose={() => setShowCustomerInfo(false)} />}
          </div>
          <div style={{ height: CHART_DIMENSIONS.height.sm }}>
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={pieData}
                  cx="50%"
                  cy="50%"
                  innerRadius={40}
                  outerRadius={70}
                  dataKey="value"
                  nameKey="name"
                  label={({ name, percent }) =>
                    `${name}: ${((percent ?? 0) * 100).toFixed(0)}%`
                  }
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
        </div>

        {/* AOV Trend Line Chart */}
        <div>
          <div className="relative">
            <h4 className="text-sm font-medium text-slate-400 mb-2 flex items-center">
              Average Order Value Trend
              <InfoButton onClick={() => setShowAovInfo(!showAovInfo)} />
            </h4>
            {showAovInfo && <AOVInfoTooltip onClose={() => setShowAovInfo(false)} />}
          </div>
          <div style={{ height: CHART_DIMENSIONS.height.sm }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={aovData} margin={CHART_DIMENSIONS.margin.default}>
                <CartesianGrid {...GRID_PROPS} />
                <XAxis
                  dataKey="date"
                  {...X_AXIS_PROPS}
                  fontSize={CHART_DIMENSIONS.fontSize.xs}
                />
                <YAxis
                  {...Y_AXIS_PROPS}
                  fontSize={CHART_DIMENSIONS.fontSize.xs}
                  tickFormatter={formatAxisK}
                />
                <Tooltip
                  contentStyle={TOOLTIP_STYLE}
                  formatter={(value) => [formatCurrency(Number(value) || 0), 'AOV']}
                />
                <Line
                  type="monotone"
                  dataKey="aov"
                  stroke={CUSTOMER_COLORS.new}
                  {...LINE_PROPS}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* Metrics */}
      {metrics && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mt-4 pt-4 border-t border-slate-700">
          <MetricCard
            label="New Customers"
            value={formatNumber(metrics.newCustomers ?? 0)}
            colorClass="text-blue-400"
          />
          <MetricCard
            label="Returning"
            value={formatNumber(metrics.returningCustomers ?? 0)}
            colorClass="text-purple-400"
          />
          <MetricCard
            label="Repeat Rate"
            value={formatPercent(metrics.repeatRate ?? 0)}
            colorClass="text-green-400"
          />
          <MetricCard
            label="Avg Order Value"
            value={formatCurrency(metrics.averageOrderValue ?? 0)}
            colorClass="text-orange-400"
          />
        </div>
      )}
    </ChartContainer>
  )
})
