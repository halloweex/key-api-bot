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
  Legend,
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
  LEGEND_PROPS,
  PIE_PROPS,
  formatAxisK,
  formatPieLabel,
} from './config'
import { useExpenseSummary, useProfitAnalysis } from '../../hooks'
import { formatCurrency, formatPercent } from '../../utils/formatters'
import { EXPENSE_COLORS, CATEGORY_COLORS } from '../../utils/colors'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ExpenseDataPoint {
  name: string
  value: number
  color: string
  [key: string]: string | number  // Recharts compatibility
}

interface ProfitDataPoint {
  date: string
  revenue: number
  expenses: number
  profit: number
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
        <p className="text-xs text-slate-600 font-medium">{label}</p>
        <p className={`text-xl font-bold ${colorClass}`}>{value}</p>
      </CardContent>
    </Card>
  )
})

// ─── Component ───────────────────────────────────────────────────────────────

export const ExpensesChart = memo(function ExpensesChart() {
  const {
    data: expenseData,
    isLoading: loadingExpense,
    error: expenseError,
    refetch: refetchExpense,
  } = useExpenseSummary()

  const {
    data: profitData,
    isLoading: loadingProfit,
  } = useProfitAnalysis()

  const isLoading = loadingExpense || loadingProfit
  const error = expenseError

  const expenseByType = useMemo<ExpenseDataPoint[]>(() => {
    if (!expenseData?.byType?.labels?.length) return []
    return expenseData.byType.labels.map((label, index) => ({
      name: label || 'Unknown',
      value: expenseData.byType.data?.[index] ?? 0,
      color: expenseData.byType.backgroundColor?.[index]
        ?? CATEGORY_COLORS[index % CATEGORY_COLORS.length],
    }))
  }, [expenseData])

  const profitChartData = useMemo<ProfitDataPoint[]>(() => {
    if (!profitData?.labels?.length) return []
    return profitData.labels.map((label, index) => ({
      date: label,
      revenue: profitData.revenue?.[index] ?? 0,
      expenses: profitData.expenses?.[index] ?? 0,
      profit: profitData.profit?.[index] ?? 0,
    }))
  }, [profitData])

  const metrics = expenseData?.metrics
  const isEmpty = !isLoading && expenseByType.length === 0 && profitChartData.length === 0

  return (
    <ChartContainer
      title="Expenses & Profit"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetchExpense}
      isEmpty={isEmpty}
      emptyMessage="No expense data available"
      height="md"
      ariaLabel="Charts showing expense breakdown and profit analysis"
    >
      {/* Metrics */}
      {metrics && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-4">
          <MetricCard
            label="Total Expenses"
            value={formatCurrency(metrics.totalExpenses ?? 0)}
            colorClass="text-red-400"
          />
          <MetricCard
            label="Gross Profit"
            value={formatCurrency(metrics.grossProfit ?? 0)}
            colorClass="text-green-400"
          />
          <MetricCard
            label="Profit Margin"
            value={formatPercent(metrics.profitMargin ?? 0)}
            colorClass="text-blue-400"
          />
          <MetricCard
            label="Orders w/ Expenses"
            value={String(metrics.ordersWithExpenses ?? 0)}
            colorClass="text-purple-400"
          />
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Expenses by Type */}
        <div>
          <h4 className="text-sm font-medium text-slate-700 mb-2">Expenses by Type</h4>
          <div style={{ height: CHART_DIMENSIONS.height.md - 32 }}>
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie
                  data={expenseByType}
                  cx="50%"
                  cy="50%"
                  innerRadius={40}
                  outerRadius={70}
                  dataKey="value"
                  nameKey="name"
                  label={({ percent }) => formatPieLabel(percent ?? 0)}
                  {...PIE_PROPS}
                >
                  {expenseByType.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={entry.color} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={TOOLTIP_STYLE}
                  formatter={(value, name) => [formatCurrency(Number(value) || 0), String(name)]}
                />
                <Legend
                  {...LEGEND_PROPS}
                  wrapperStyle={{ ...LEGEND_PROPS.wrapperStyle, fontSize: '10px' }}
                  layout="vertical"
                  align="right"
                  verticalAlign="middle"
                />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>

        {/* Profit Analysis */}
        <div>
          <h4 className="text-sm font-medium text-slate-700 mb-2">Profit Analysis</h4>
          <div style={{ height: CHART_DIMENSIONS.height.md - 32 }}>
            <ResponsiveContainer width="100%" height="100%">
              <LineChart data={profitChartData} margin={CHART_DIMENSIONS.margin.default}>
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
                  formatter={(value, name) => [
                    formatCurrency(Number(value) || 0),
                    name === 'revenue' ? 'Revenue'
                      : name === 'expenses' ? 'Expenses'
                      : 'Profit',
                  ]}
                />
                <Legend
                  {...LEGEND_PROPS}
                  wrapperStyle={{ ...LEGEND_PROPS.wrapperStyle, fontSize: '11px' }}
                />
                <Line
                  type="monotone"
                  dataKey="revenue"
                  name="Revenue"
                  stroke={EXPENSE_COLORS.revenue}
                  {...LINE_PROPS}
                />
                <Line
                  type="monotone"
                  dataKey="expenses"
                  name="Expenses"
                  stroke={EXPENSE_COLORS.expenses}
                  {...LINE_PROPS}
                />
                <Line
                  type="monotone"
                  dataKey="profit"
                  name="Profit"
                  stroke={EXPENSE_COLORS.profit}
                  {...LINE_PROPS}
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </ChartContainer>
  )
})
