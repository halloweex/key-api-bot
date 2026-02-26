import { memo, useMemo, useState, useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { ChartContainer } from './ChartContainer'
import { formatCurrency } from '../../utils/formatters'
import { useFilterStore } from '../../store/filterStore'
import { CurrencyIcon, TrashIcon } from '../icons'

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

const CATEGORIES = ['marketing', 'salary', 'taxes', 'logistics', 'other']

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

// ─── Category Filter ─────────────────────────────────────────────────────────

interface CategoryFilterProps {
  value: string | null
  onChange: (category: string | null) => void
}

const CategoryFilter = memo(function CategoryFilter({ value, onChange }: CategoryFilterProps) {
  const { t } = useTranslation()
  return (
    <select
      value={value || ''}
      onChange={(e) => onChange(e.target.value || null)}
      className="text-xs px-2 py-1 rounded-lg border border-slate-200 bg-white text-slate-600
                 focus:outline-none focus:ring-2 focus:ring-purple-500/20 focus:border-purple-300
                 cursor-pointer hover:border-slate-300 transition-colors"
    >
      <option value="">{t('chart.allCategories')}</option>
      {CATEGORIES.map((cat) => {
        const config = categoryConfig[cat]
        return (
          <option key={cat} value={cat}>
            {config.icon} {cat.charAt(0).toUpperCase() + cat.slice(1)}
          </option>
        )
      })}
    </select>
  )
})

// ─── Note Tooltip ────────────────────────────────────────────────────────────

const NoteCell = memo(function NoteCell({ note }: { note: string | null }) {
  if (!note) {
    return <span className="text-slate-300">—</span>
  }

  const isLong = note.length > 30
  const displayText = isLong ? note.slice(0, 30) + '...' : note

  return (
    <span
      className="text-slate-500 text-xs cursor-default"
      title={note}
    >
      {displayText}
    </span>
  )
})

// ─── Delete Button ───────────────────────────────────────────────────────────

interface DeleteButtonProps {
  expenseId: number
  onDelete: (id: number) => void
  isDeleting: boolean
}

const DeleteButton = memo(function DeleteButton({ expenseId, onDelete, isDeleting }: DeleteButtonProps) {
  const { t } = useTranslation()
  return (
    <button
      onClick={() => onDelete(expenseId)}
      disabled={isDeleting}
      className="p-1.5 rounded-lg text-slate-400 hover:text-red-500 hover:bg-red-50
                 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
      title={t('chart.deleteExpense')}
    >
      {isDeleting ? (
        <span className="block w-4 h-4 border-2 border-slate-300 border-t-transparent rounded-full animate-spin" />
      ) : (
        <TrashIcon className="w-4 h-4" />
      )}
    </button>
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
          <p className={`text-xl font-bold truncate ${colorClass}`}>{value}</p>
        </div>
      </div>
    </div>
  )
})

// ─── Empty State ──────────────────────────────────────────────────────────────

const EmptyHint = memo(function EmptyHint() {
  const { t } = useTranslation()
  return (
    <div className="text-center py-8">
      <div className="w-14 h-14 mx-auto mb-4 rounded-2xl bg-gradient-to-br from-slate-100 to-slate-50 flex items-center justify-center shadow-sm text-slate-400">
        <CurrencyIcon />
      </div>
      <p className="text-sm font-medium text-slate-600 mb-1">{t('chart.noExpensesYet')}</p>
      <p className="text-xs text-slate-400">
        {t('chart.addViaChat')}
      </p>
    </div>
  )
})

// ─── Component ───────────────────────────────────────────────────────────────

export const ManualExpensesTable = memo(function ManualExpensesTable() {
  const { t } = useTranslation()
  const { period } = useFilterStore()
  const queryClient = useQueryClient()
  const [categoryFilter, setCategoryFilter] = useState<string | null>(null)
  const [deletingId, setDeletingId] = useState<number | null>(null)

  const { data, isLoading, error, refetch } = useQuery<ExpensesResponse>({
    queryKey: ['manualExpenses', period, categoryFilter],
    queryFn: async () => {
      const params = new URLSearchParams()
      if (period) params.set('period', period)
      if (categoryFilter) params.set('category', categoryFilter)
      params.set('limit', '50')
      const response = await fetch(`/api/expenses?${params}`)
      if (!response.ok) throw new Error('Failed to fetch expenses')
      return response.json()
    },
    staleTime: 30_000,
  })

  const deleteMutation = useMutation({
    mutationFn: async (expenseId: number) => {
      const response = await fetch(`/api/expenses/${expenseId}`, {
        method: 'DELETE',
      })
      if (!response.ok) throw new Error('Failed to delete expense')
      return response.json()
    },
    onMutate: (expenseId) => {
      setDeletingId(expenseId)
    },
    onSuccess: () => {
      // Invalidate query to refetch data
      queryClient.invalidateQueries({ queryKey: ['manualExpenses'] })
    },
    onSettled: () => {
      setDeletingId(null)
    },
  })

  const handleDelete = useCallback((expenseId: number) => {
    if (confirm(t('chart.deleteConfirm'))) {
      deleteMutation.mutate(expenseId)
    }
  }, [deleteMutation])

  const isEmpty = !isLoading && (!data?.expenses || data.expenses.length === 0)

  const topCategories = useMemo(() => {
    if (!data?.summary?.by_category) return []
    return data.summary.by_category.slice(0, 4)
  }, [data])

  // Custom empty state since we have specific instructions
  if (isEmpty && !isLoading && !error) {
    return (
      <ChartContainer
        title={t('chart.manualExpenses')}
        titleExtra={
          <CategoryFilter value={categoryFilter} onChange={setCategoryFilter} />
        }
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
      title={t('chart.manualExpenses')}
      titleExtra={
        <div className="flex items-center gap-3">
          <span className="text-xs text-slate-400 font-normal hidden sm:inline">
            {t('chart.addViaChatShort')}
          </span>
          <CategoryFilter value={categoryFilter} onChange={setCategoryFilter} />
        </div>
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
            label={t('chart.totalExpenses')}
            value={formatCurrency(data.summary.total)}
            icon="💰"
            colorClass="text-red-600"
            bgClass="bg-gradient-to-br from-red-50 to-red-100/50 border-red-200"
          />
          <MetricCard
            label={t('chart.transactions')}
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
                {t('chart.date')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                {t('chart.category')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                {t('chart.type')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden md:table-cell">
                {t('chart.note')}
              </th>
              <th className="text-right py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                {t('chart.amount')}
              </th>
              <th className="w-12 py-3 px-2"></th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {data?.expenses?.map((expense) => (
              <tr
                key={expense.id}
                className="hover:bg-slate-50 transition-colors group"
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
                <td className="py-3 px-4 hidden md:table-cell max-w-[200px]">
                  <NoteCell note={expense.note} />
                </td>
                <td className="py-3 px-4 text-right text-red-600 font-semibold whitespace-nowrap">
                  -{formatCurrency(expense.amount)}
                </td>
                <td className="py-2 px-2">
                  <DeleteButton
                    expenseId={expense.id}
                    onDelete={handleDelete}
                    isDeleting={deletingId === expense.id}
                  />
                </td>
              </tr>
            ))}
          </tbody>
          {data?.expenses && data.expenses.length > 0 && (
            <tfoot>
              <tr className="bg-slate-50 border-t border-slate-200">
                <td colSpan={4} className="py-3 px-4 text-right text-slate-600 font-semibold hidden md:table-cell">
                  {t('common.total')}
                </td>
                <td colSpan={3} className="py-3 px-4 text-right text-slate-600 font-semibold md:hidden">
                  {t('common.total')}
                </td>
                <td className="py-3 px-4 text-right text-red-600 font-bold whitespace-nowrap">
                  -{formatCurrency(data.summary.total)}
                </td>
                <td></td>
              </tr>
            </tfoot>
          )}
        </table>
      </div>
    </ChartContainer>
  )
})
