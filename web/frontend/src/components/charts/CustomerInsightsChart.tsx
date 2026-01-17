import { useMemo, memo } from 'react'
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

// ─── Component ───────────────────────────────────────────────────────────────

export const CustomerInsightsChart = memo(function CustomerInsightsChart() {
  const { data, isLoading, error, refetch } = useCustomerInsights()

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
          <h4 className="text-sm font-medium text-slate-400 mb-2">New vs Returning</h4>
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
          <h4 className="text-sm font-medium text-slate-400 mb-2">Average Order Value Trend</h4>
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
