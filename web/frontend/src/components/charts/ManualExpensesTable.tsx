import { memo, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChartContainer } from './ChartContainer'
import { formatCurrency } from '../../utils/formatters'
import { useFilterStore } from '../../store/filterStore'
import { CurrencyIcon } from '../icons'

// ─── Types ───────────────────────────────────────────────────────────────────

interface Expense {
  id: number
  expense_date: string
  category: string
  expense_type: string
  amount: number
  currency: string
  note: string | null
  created_at: string
  updated_at: string | null
}

interface ExpenseSummary {
  total: number
  count: number
  by_category: Array<{
    category: string
    total: number
    count: number
  }>
}

interface ExpensesResponse {
  expenses: Expense[]
  summary: ExpenseSummary
  period: string | null
  category: string | null
}

// ─── Category Configuration ───────────────────────────────────────────────────

const categoryConfig: Record<string, { bg: string; text: string; icon: string }> = {
  marketing: {
    bg: 'bg-purple-100',
    text: 'text-purple-700',
    icon: '📣',
  },
  salary: {
    bg: 'bg-blue-100',
    text: 'text-blue-700',
    icon: '👥',
  },
  taxes: {
    bg: 'bg-red-100',
    text: 'text-red-700',
    icon: '🏛️',
  },
  logistics: {
    bg: 'bg-amber-100',
    text: 'text-amber-700',
    icon: '📦',
  },
  other: {
    bg: 'bg-slate-100',
    text: 'text-slate-700',
    icon: '📋',
  },
}

// ─── Category Badge ──────────────────────────────────────────────────────────

const CategoryBadge = memo(function CategoryBadge({ category }: { category: string }) {
  const config = categoryConfig[category] || categoryConfig.other
  const displayName = category.charAt(0).toUpperCase() + category.slice(1)

  return (
    <span className={`inline-flex items-center gap-1 px-2.5 py-1 text-xs font-medium rounded-full ${config.bg} ${config.text}`}>
      <span>{config.icon}</span>
      <span>{displayName}</span>
    </span>
  )
})

// ─── Summary Metric Card ──────────────────────────────────────────────────────

interface MetricCardProps {
  label: string
  value: string
  icon?: string
  colorClass: string
  bgClass: string
}

const MetricCard = memo(function MetricCard({ label, value, icon, colorClass, bgClass }: MetricCardProps) {
  return (
    <div className={`rounded-xl p-4 border ${bgClass}`}>
      <div className="flex items-start gap-3">
        {icon && (
          <div className={`p-2 rounded-lg bg-white/60 ${colorClass} text-lg`}>
            {icon}
          </div>
        )}
        <div className="flex-1 min-w-0">
          <p className="text-xs text-slate-600 font-medium">{label}</p>
          <p className={`text-lg lg:text-xl font-bold truncate ${colorClass}`}>{value}</p>
        </div>
      </div>
    </div>
  )
})

// ─── Empty State ──────────────────────────────────────────────────────────────

const EmptyHint = memo(function EmptyHint() {
  return (
    <div className="text-center py-8">
      <div className="w-14 h-14 mx-auto mb-4 rounded-2xl bg-gradient-to-br from-slate-100 to-slate-50 flex items-center justify-center shadow-sm text-slate-400">
        <CurrencyIcon />
      </div>
      <p className="text-sm font-medium text-slate-600 mb-1">No expenses yet</p>
      <p className="text-xs text-slate-400">
        Use chat to add: <span className="font-mono bg-slate-100 px-1.5 py-0.5 rounded">facebook ads 22000</span>
      </p>
    </div>
  )
})

// ─── Component ───────────────────────────────────────────────────────────────

export const ManualExpensesTable = memo(function ManualExpensesTable() {
  const { period } = useFilterStore()

  const { data, isLoading, error, refetch } = useQuery<ExpensesResponse>({
    queryKey: ['manualExpenses', period],
    queryFn: async () => {
      const params = new URLSearchParams()
      if (period) params.set('period', period)
      params.set('limit', '50')
      const response = await fetch(`/api/expenses?${params}`)
      if (!response.ok) throw new Error('Failed to fetch expenses')
      return response.json()
    },
    staleTime: 30_000,
  })

  const isEmpty = !isLoading && (!data?.expenses || data.expenses.length === 0)

  const topCategories = useMemo(() => {
    if (!data?.summary?.by_category) return []
    return data.summary.by_category.slice(0, 4)
  }, [data])

  // Custom empty state since we have specific instructions
  if (isEmpty && !isLoading && !error) {
    return (
      <ChartContainer
        title="Manual Expenses"
        isLoading={false}
        error={null}
        height="md"
        ariaLabel="Table showing manual business expenses"
      >
        <EmptyHint />
      </ChartContainer>
    )
  }

  return (
    <ChartContainer
      title="Manual Expenses"
      titleExtra={
        <span className="text-xs text-slate-400 font-normal ml-2">
          Add via chat: "facebook ads 22k, salary 45k"
        </span>
      }
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      height="auto"
      ariaLabel="Table showing manual business expenses"
    >
      {/* Summary Cards */}
      {data?.summary && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-5">
          <MetricCard
            label="Total Expenses"
            value={formatCurrency(data.summary.total)}
            icon="💰"
            colorClass="text-red-600"
            bgClass="bg-gradient-to-br from-red-50 to-red-100/50 border-red-200"
          />
          <MetricCard
            label="Transactions"
            value={String(data.summary.count)}
            icon="📝"
            colorClass="text-slate-700"
            bgClass="bg-gradient-to-br from-slate-50 to-slate-100/50 border-slate-200"
          />
          {topCategories.slice(0, 2).map((cat) => {
            const config = categoryConfig[cat.category] || categoryConfig.other
            return (
              <MetricCard
                key={cat.category}
                label={cat.category.charAt(0).toUpperCase() + cat.category.slice(1)}
                value={formatCurrency(cat.total)}
                icon={config.icon}
                colorClass={config.text}
                bgClass={`bg-gradient-to-br from-white to-slate-50 border-slate-200`}
              />
            )
          })}
        </div>
      )}

      {/* Table */}
      <div className="overflow-auto rounded-lg border border-slate-200 max-h-[400px]">
        <table className="w-full text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="bg-slate-50 border-b border-slate-200">
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                Date
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                Category
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                Type
              </th>
              <th className="text-right py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                Amount
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {data?.expenses?.map((expense) => (
              <tr
                key={expense.id}
                className="hover:bg-slate-50 transition-colors"
              >
                <td className="py-3 px-4 text-slate-600 whitespace-nowrap">
                  {new Date(expense.expense_date).toLocaleDateString('uk-UA', {
                    day: '2-digit',
                    month: '2-digit',
                    year: 'numeric',
                  })}
                </td>
                <td className="py-3 px-4">
                  <CategoryBadge category={expense.category} />
                </td>
                <td className="py-3 px-4 text-slate-800 font-medium">
                  {expense.expense_type}
                </td>
                <td className="py-3 px-4 text-right text-red-600 font-semibold whitespace-nowrap">
                  -{formatCurrency(expense.amount)}
                </td>
              </tr>
            ))}
          </tbody>
          {data?.expenses && data.expenses.length > 0 && (
            <tfoot>
              <tr className="bg-slate-50 border-t border-slate-200">
                <td colSpan={3} className="py-3 px-4 text-right text-slate-600 font-semibold">
                  Total:
                </td>
                <td className="py-3 px-4 text-right text-red-600 font-bold whitespace-nowrap">
                  -{formatCurrency(data.summary.total)}
                </td>
              </tr>
            </tfoot>
          )}
        </table>
      </div>
    </ChartContainer>
  )
})
