import { memo, useState, useCallback, useMemo } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ChartContainer } from '../charts/ChartContainer'
import { Card, CardContent } from '../ui'
import { TrashIcon } from '../icons'
import { formatCurrency } from '../../utils/formatters'
import { useSummary, useTrafficROAS, useCreateExpense, useDeleteExpense } from '../../hooks'
import { useFilterStore } from '../../store/filterStore'
import type { CreateExpenseRequest } from '../../types/api'

// ─── Constants ────────────────────────────────────────────────────────────────

const PLATFORMS = [
  { value: 'facebook', label: 'Facebook', icon: '\uD83D\uDCD8' },
  { value: 'tiktok', label: 'TikTok', icon: '\uD83C\uDFB5' },
  { value: 'google', label: 'Google', icon: '\uD83D\uDD0D' },
] as const

const BONUS_TIERS = [
  { label: '> 7.0x', bonus: '+30%' },
  { label: '6.0 \u2013 7.0x', bonus: '+20%' },
  { label: '5.0 \u2013 6.0x', bonus: '+10%' },
  { label: '4.0 \u2013 5.0x', bonus: 'Base rate' },
  { label: '< 4.0x', bonus: 'No bonus' },
]

function roasColor(roas: number | null): string {
  if (roas === null) return 'text-slate-400'
  if (roas >= 5) return 'text-emerald-600'
  if (roas >= 3) return 'text-amber-600'
  return 'text-red-600'
}

function roasBg(roas: number | null): string {
  if (roas === null) return 'from-slate-50 to-slate-100/50 border-slate-200'
  if (roas >= 5) return 'from-emerald-50 to-emerald-100/50 border-emerald-200'
  if (roas >= 3) return 'from-amber-50 to-amber-100/50 border-amber-200'
  return 'from-red-50 to-red-100/50 border-red-200'
}

// ─── Blended ROAS Card ──────────────────────────────────────────────────────

interface BlendedCardProps {
  revenue: number
  spend: number
  roas: number | null
  bonusTier: string
}

const BlendedROASCard = memo(function BlendedROASCard({ revenue, spend, roas, bonusTier }: BlendedCardProps) {
  return (
    <div className={`rounded-xl p-4 sm:p-5 border bg-gradient-to-br ${roasBg(roas)}`}>
      <h4 className="text-sm font-semibold text-slate-700 mb-3">Blended ROAS</h4>
      <div className="grid grid-cols-2 gap-3 mb-3">
        <div>
          <p className="text-xs text-slate-500">Total Revenue</p>
          <p className="text-lg font-bold text-slate-800">{formatCurrency(revenue)}</p>
        </div>
        <div>
          <p className="text-xs text-slate-500">Total Ad Spend</p>
          <p className="text-lg font-bold text-slate-800">{spend > 0 ? formatCurrency(spend) : '\u2014'}</p>
        </div>
      </div>
      <div className="flex items-end gap-4 pt-3 border-t border-slate-200/60">
        <div>
          <p className="text-xs text-slate-500">ROAS</p>
          <p className={`text-2xl font-bold ${roasColor(roas)}`}>
            {roas !== null ? `${roas}x` : '\u2014'}
          </p>
        </div>
        <div>
          <p className="text-xs text-slate-500">Bonus Tier</p>
          <span className={`inline-block px-2.5 py-1 text-sm font-semibold rounded-lg ${roasColor(roas)} bg-white/60`}>
            {bonusTier}
          </span>
        </div>
      </div>
    </div>
  )
})

// ─── Bonus Tier Table ───────────────────────────────────────────────────────

const BonusTierTable = memo(function BonusTierTable({ currentTier }: { currentTier: string }) {
  return (
    <div className="overflow-hidden rounded-lg border border-slate-200">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-slate-50">
            <th className="text-left py-2 px-3 text-xs font-semibold text-slate-600 uppercase tracking-wide">
              Blended ROAS
            </th>
            <th className="text-left py-2 px-3 text-xs font-semibold text-slate-600 uppercase tracking-wide">
              Bonus
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {BONUS_TIERS.map((tier) => {
            const isActive = tier.bonus === currentTier
            return (
              <tr
                key={tier.label}
                className={isActive ? 'bg-emerald-50 font-semibold' : 'hover:bg-slate-50'}
              >
                <td className={`py-2 px-3 ${isActive ? 'text-emerald-800' : 'text-slate-700'}`}>
                  {tier.label}
                </td>
                <td className={`py-2 px-3 ${isActive ? 'text-emerald-700' : 'text-slate-600'}`}>
                  {isActive && <span className="mr-1">{'\u2192'}</span>}
                  {tier.bonus}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
})

// ─── Platform Card ──────────────────────────────────────────────────────────

interface PlatformCardProps {
  platform: string
  icon: string
  paidRevenue: number
  spend: number
  roas: number | null
}

const PlatformCard = memo(function PlatformCard({ platform, icon, paidRevenue, spend, roas }: PlatformCardProps) {
  return (
    <div className="rounded-xl p-4 border border-slate-200 bg-white">
      <div className="flex items-center gap-2 mb-3">
        <span className="text-lg">{icon}</span>
        <h5 className="text-sm font-semibold text-slate-800">{platform}</h5>
      </div>
      <div className="space-y-1.5 text-sm">
        <div className="flex justify-between">
          <span className="text-slate-500">Ad Spend</span>
          <span className="font-medium text-slate-800">{spend > 0 ? formatCurrency(spend) : '\u2014'}</span>
        </div>
        <div className="flex justify-between">
          <span className="text-slate-500">Paid Revenue</span>
          <span className="font-medium text-slate-800">{paidRevenue > 0 ? formatCurrency(paidRevenue) : '\u2014'}</span>
        </div>
        <div className="flex justify-between pt-1.5 border-t border-slate-100">
          <span className="text-slate-500">Channel ROAS</span>
          <span className={`font-bold ${roasColor(roas)}`}>
            {roas !== null ? `${roas}x` : '\u2014'}
          </span>
        </div>
      </div>
    </div>
  )
})

// ─── Ad Spend Input Form ────────────────────────────────────────────────────

const AdSpendInput = memo(function AdSpendInput() {
  const [platform, setPlatform] = useState('facebook')
  const [date, setDate] = useState(() => new Date().toISOString().split('T')[0])
  const [amount, setAmount] = useState('')
  const createExpense = useCreateExpense()

  const platformConfig = PLATFORMS.find(p => p.value === platform)
  const expenseType = platformConfig ? `${platformConfig.label} Ads` : 'Ads'

  const handleSubmit = useCallback((e: React.FormEvent) => {
    e.preventDefault()
    const numAmount = parseFloat(amount)
    if (!numAmount || numAmount <= 0) return

    const data: CreateExpenseRequest = {
      expense_date: date,
      category: 'marketing',
      expense_type: expenseType,
      amount: numAmount,
      platform,
    }
    createExpense.mutate(data, {
      onSuccess: () => setAmount(''),
    })
  }, [platform, date, amount, expenseType, createExpense])

  return (
    <form onSubmit={handleSubmit} className="flex flex-wrap items-end gap-2 mt-3">
      <div className="flex-shrink-0">
        <label className="block text-xs text-slate-500 mb-1">Platform</label>
        <select
          value={platform}
          onChange={(e) => setPlatform(e.target.value)}
          className="text-sm px-2.5 py-2 rounded-lg border border-slate-200 bg-white
                     focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-300"
        >
          {PLATFORMS.map(p => (
            <option key={p.value} value={p.value}>{p.icon} {p.label}</option>
          ))}
        </select>
      </div>
      <div className="flex-shrink-0">
        <label className="block text-xs text-slate-500 mb-1">Date</label>
        <input
          type="date"
          value={date}
          onChange={(e) => setDate(e.target.value)}
          className="text-sm px-2.5 py-2 rounded-lg border border-slate-200 bg-white
                     focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-300"
        />
      </div>
      <div className="flex-1 min-w-[120px]">
        <label className="block text-xs text-slate-500 mb-1">Amount (UAH)</label>
        <input
          type="number"
          value={amount}
          onChange={(e) => setAmount(e.target.value)}
          placeholder="e.g. 50000"
          min="1"
          step="1"
          className="w-full text-sm px-2.5 py-2 rounded-lg border border-slate-200 bg-white
                     focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-300"
        />
      </div>
      <button
        type="submit"
        disabled={createExpense.isPending || !amount || parseFloat(amount) <= 0}
        className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-lg
                   hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed
                   transition-colors flex-shrink-0"
      >
        {createExpense.isPending ? 'Adding...' : 'Add'}
      </button>
    </form>
  )
})

// ─── Recent Entries List ────────────────────────────────────────────────────

interface MarketingExpense {
  id: number
  expense_date: string
  category: string
  expense_type: string
  amount: number
  platform: string | null
}

interface ExpenseListResponse {
  expenses: MarketingExpense[]
}

const RecentEntries = memo(function RecentEntries() {
  const deleteExpense = useDeleteExpense()
  const [deletingId, setDeletingId] = useState<number | null>(null)
  const { period } = useFilterStore()

  const { data } = useQuery<ExpenseListResponse>({
    queryKey: ['manualExpenses', period, 'marketing'],
    queryFn: async () => {
      const params = new URLSearchParams()
      if (period) params.set('period', period)
      params.set('category', 'marketing')
      params.set('limit', '20')
      const response = await fetch(`/api/expenses?${params}`)
      if (!response.ok) throw new Error('Failed to fetch expenses')
      return response.json()
    },
    staleTime: 30_000,
  })

  const handleDelete = useCallback((id: number) => {
    setDeletingId(id)
    deleteExpense.mutate(id, {
      onSettled: () => setDeletingId(null),
    })
  }, [deleteExpense])

  const marketingExpenses = useMemo(
    () => (data?.expenses ?? []).filter(e => e.category === 'marketing' && e.platform),
    [data],
  )

  if (!marketingExpenses.length) return null

  return (
    <div className="mt-3 overflow-hidden rounded-lg border border-slate-200">
      <table className="w-full text-sm">
        <thead>
          <tr className="bg-slate-50">
            <th className="text-left py-2 px-3 text-xs font-semibold text-slate-600">Date</th>
            <th className="text-left py-2 px-3 text-xs font-semibold text-slate-600">Platform</th>
            <th className="text-right py-2 px-3 text-xs font-semibold text-slate-600">Amount</th>
            <th className="w-10 py-2 px-2"></th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {marketingExpenses.map((exp) => {
            const platformInfo = PLATFORMS.find(p => p.value === exp.platform)
            return (
              <tr key={exp.id} className="hover:bg-slate-50">
                <td className="py-2 px-3 text-slate-600">
                  {new Date(exp.expense_date).toLocaleDateString('uk-UA', { day: '2-digit', month: '2-digit' })}
                </td>
                <td className="py-2 px-3 text-slate-700 capitalize">
                  {platformInfo?.icon} {exp.platform}
                </td>
                <td className="py-2 px-3 text-right font-medium text-slate-800">
                  {formatCurrency(exp.amount)}
                </td>
                <td className="py-1 px-2">
                  <button
                    onClick={() => handleDelete(exp.id)}
                    disabled={deletingId === exp.id}
                    className="p-1.5 rounded-lg text-slate-400 hover:text-red-500 hover:bg-red-50
                               transition-colors disabled:opacity-50"
                    title="Delete"
                  >
                    {deletingId === exp.id ? (
                      <span className="block w-4 h-4 border-2 border-slate-300 border-t-transparent rounded-full animate-spin" />
                    ) : (
                      <TrashIcon className="w-4 h-4" />
                    )}
                  </button>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
})

// ─── Main Component ─────────────────────────────────────────────────────────

export const ROASSection = memo(function ROASSection() {
  const { data: summary } = useSummary()
  const { data: roasData, isLoading, error, refetch } = useTrafficROAS()
  const [showInput, setShowInput] = useState(false)

  const totalRevenue = summary?.totalRevenue ?? 0
  const blended = roasData?.blended
  const byPlatform = roasData?.by_platform ?? {}
  const bonusTier = roasData?.bonus_tier ?? 'No bonus'
  const hasSpendData = roasData?.has_spend_data ?? false

  const displayRevenue = totalRevenue
  const displaySpend = blended?.spend ?? 0
  const displayRoas = blended?.roas ?? null

  const platformCards = useMemo(() => {
    return PLATFORMS.map(p => {
      const data = byPlatform[p.value]
      return {
        ...p,
        paidRevenue: data?.paid_revenue ?? 0,
        spend: data?.spend ?? 0,
        roas: data?.roas ?? null,
      }
    })
  }, [byPlatform])

  return (
    <ChartContainer
      title="ROAS Calculator"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      height="auto"
      ariaLabel="Blended ROAS calculator with bonus tier"
      action={
        <button
          onClick={() => setShowInput(!showInput)}
          className="text-xs px-3 py-1.5 rounded-lg border border-slate-200 text-slate-600
                     hover:bg-slate-50 hover:border-slate-300 transition-colors font-medium"
        >
          {showInput ? 'Hide Input' : 'Edit Ad Spend'}
        </button>
      }
    >
      <div className="space-y-5">
        {/* Blended ROAS */}
        <BlendedROASCard
          revenue={displayRevenue}
          spend={displaySpend}
          roas={displayRoas}
          bonusTier={bonusTier}
        />

        {/* Bonus Tier Table */}
        <BonusTierTable currentTier={bonusTier} />

        {/* Platform Breakdown */}
        {hasSpendData && (
          <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
            {platformCards.map(p => (
              <PlatformCard
                key={p.value}
                platform={p.label}
                icon={p.icon}
                paidRevenue={p.paidRevenue}
                spend={p.spend}
                roas={p.roas}
              />
            ))}
          </div>
        )}

        {/* Ad Spend Input (collapsible) */}
        {showInput && (
          <Card>
            <CardContent>
              <h4 className="text-sm font-semibold text-slate-700 mb-1">Add Ad Spend</h4>
              <p className="text-xs text-slate-400 mb-2">Enter daily ad spend per platform</p>
              <AdSpendInput />
              <RecentEntries />
            </CardContent>
          </Card>
        )}

        {/* Empty state hint */}
        {!hasSpendData && !showInput && (
          <div className="text-center py-4">
            <p className="text-sm text-slate-500">
              No ad spend data for this period.{' '}
              <button
                onClick={() => setShowInput(true)}
                className="text-blue-600 hover:text-blue-700 font-medium"
              >
                Add ad spend
              </button>{' '}
              to calculate ROAS.
            </p>
          </div>
        )}
      </div>
    </ChartContainer>
  )
})
