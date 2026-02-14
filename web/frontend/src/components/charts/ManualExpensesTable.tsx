import { memo, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChartContainer } from './ChartContainer'
import { formatCurrency } from '../../utils/formatters'
import { useFilterStore } from '../../store/filterStore'

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

// ─── Category Badge ──────────────────────────────────────────────────────────

const categoryColors: Record<string, string> = {
  marketing: 'bg-purple-500/20 text-purple-300 border-purple-500/30',
  salary: 'bg-blue-500/20 text-blue-300 border-blue-500/30',
  taxes: 'bg-red-500/20 text-red-300 border-red-500/30',
  logistics: 'bg-amber-500/20 text-amber-300 border-amber-500/30',
  other: 'bg-slate-500/20 text-slate-300 border-slate-500/30',
}

const CategoryBadge = memo(function CategoryBadge({ category }: { category: string }) {
  const colors = categoryColors[category] || categoryColors.other
  return (
    <span className={`px-2 py-0.5 text-xs rounded-full border ${colors}`}>
      {category}
    </span>
  )
})

// ─── Summary Card ────────────────────────────────────────────────────────────

interface SummaryCardProps {
  label: string
  value: string
  colorClass?: string
}

const SummaryCard = memo(function SummaryCard({ label, value, colorClass = 'text-white' }: SummaryCardProps) {
  return (
    <div className="bg-slate-700/50 rounded-lg p-3">
      <p className="text-xs text-slate-400">{label}</p>
      <p className={`text-lg font-semibold ${colorClass}`}>{value}</p>
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
    return data.summary.by_category.slice(0, 3)
  }, [data])

  return (
    <ChartContainer
      title="Manual Expenses — Add via chat: 'facebook ads 22k, salary 45k'"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      emptyMessage="No expenses yet. Use chat to add: 'facebook ads 22000'"
      height="md"
      ariaLabel="Table showing manual business expenses"
    >
      {/* Summary */}
      {data?.summary && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-4">
          <SummaryCard
            label="Total Expenses"
            value={formatCurrency(data.summary.total)}
            colorClass="text-red-400"
          />
          <SummaryCard
            label="Count"
            value={String(data.summary.count)}
            colorClass="text-slate-200"
          />
          {topCategories.slice(0, 2).map((cat) => (
            <SummaryCard
              key={cat.category}
              label={cat.category.charAt(0).toUpperCase() + cat.category.slice(1)}
              value={formatCurrency(cat.total)}
              colorClass="text-slate-200"
            />
          ))}
        </div>
      )}

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-700">
              <th className="text-left py-2 px-3 text-slate-400 font-medium">Date</th>
              <th className="text-left py-2 px-3 text-slate-400 font-medium">Category</th>
              <th className="text-left py-2 px-3 text-slate-400 font-medium">Type</th>
              <th className="text-right py-2 px-3 text-slate-400 font-medium">Amount</th>
            </tr>
          </thead>
          <tbody>
            {data?.expenses?.map((expense) => (
              <tr
                key={expense.id}
                className="border-b border-slate-700/50 hover:bg-slate-700/30 transition-colors"
              >
                <td className="py-2 px-3 text-slate-300">
                  {new Date(expense.expense_date).toLocaleDateString('uk-UA', {
                    day: '2-digit',
                    month: '2-digit',
                    year: 'numeric',
                  })}
                </td>
                <td className="py-2 px-3">
                  <CategoryBadge category={expense.category} />
                </td>
                <td className="py-2 px-3 text-slate-200">{expense.expense_type}</td>
                <td className="py-2 px-3 text-right text-red-400 font-medium">
                  {formatCurrency(expense.amount)}
                </td>
              </tr>
            ))}
          </tbody>
          {data?.expenses && data.expenses.length > 0 && (
            <tfoot>
              <tr className="border-t border-slate-600">
                <td colSpan={3} className="py-2 px-3 text-right text-slate-400 font-medium">
                  Total:
                </td>
                <td className="py-2 px-3 text-right text-red-400 font-bold">
                  {formatCurrency(data.summary.total)}
                </td>
              </tr>
            </tfoot>
          )}
        </table>
      </div>
    </ChartContainer>
  )
})
