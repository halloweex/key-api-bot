import { memo, useState, useCallback, useRef, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from '../charts/ChartContainer'
import { formatCurrency } from '../../utils/formatters'
import { useTrafficTransactions } from '../../hooks'
import { useQueryParams } from '../../store/filterStore'
import type { TrafficEvidence, TrafficTransaction } from '../../types/api'

// ─── Traffic Type Config ─────────────────────────────────────────────────────

const TRAFFIC_TYPES = [
  { value: '', labelKey: 'traffic.allTypes' },
  { value: 'paid_confirmed', labelKey: 'traffic.paid' },
  { value: 'paid_likely', labelKey: 'traffic.paidLikely' },
  { value: 'manager', labelKey: 'traffic.salesManager' },
  { value: 'organic', labelKey: 'traffic.organic' },
  { value: 'pixel_only', labelKey: 'traffic.pixelOnly' },
  { value: 'unknown', labelKey: 'traffic.unknown' },
] as const

const trafficBadgeConfig: Record<string, { bg: string; text: string; labelKey: string }> = {
  paid_confirmed: { bg: 'bg-blue-100', text: 'text-blue-700', labelKey: 'traffic.paid' },
  paid_likely: { bg: 'bg-blue-50', text: 'text-blue-500', labelKey: 'traffic.paidLikely' },
  manager: { bg: 'bg-cyan-100', text: 'text-cyan-700', labelKey: 'traffic.manager' },
  organic: { bg: 'bg-green-100', text: 'text-green-700', labelKey: 'traffic.organic' },
  pixel_only: { bg: 'bg-orange-100', text: 'text-orange-700', labelKey: 'traffic.pixelOnly' },
  unknown: { bg: 'bg-purple-100', text: 'text-purple-700', labelKey: 'traffic.unknown' },
}

const PLATFORMS = [
  { value: '', labelKey: 'traffic.allPlatforms' },
  { value: 'facebook', labelKey: 'traffic.facebook' },
  { value: 'instagram', labelKey: 'traffic.instagram' },
  { value: 'google', labelKey: 'traffic.google' },
  { value: 'tiktok', labelKey: 'traffic.tiktok' },
  { value: 'email', labelKey: 'traffic.email' },
  { value: 'telegram', labelKey: 'traffic.telegram' },
  { value: 'manager', labelKey: 'traffic.manager' },
  { value: 'other', labelKey: 'traffic.otherPlatform' },
] as const

const PAGE_SIZE = 50

// ─── Sub-components ──────────────────────────────────────────────────────────

const TrafficBadge = memo(function TrafficBadge({ type }: { type: string }) {
  const { t } = useTranslation()
  const config = trafficBadgeConfig[type] || trafficBadgeConfig.unknown
  return (
    <span className={`inline-flex px-2 py-0.5 text-xs font-medium rounded-full ${config.bg} ${config.text}`}>
      {t(config.labelKey)}
    </span>
  )
})

const EvidencePills = memo(function EvidencePills({ evidence }: { evidence: TrafficEvidence[] }) {
  if (!evidence.length) {
    return <span className="text-slate-300">--</span>
  }

  const visible = evidence.slice(0, 3)
  const remaining = evidence.length - 3

  return (
    <div className="flex flex-wrap gap-1">
      {visible.map((e, i) => {
        const displayValue = e.value.length > 20 ? e.value.slice(0, 20) + '...' : e.value
        return (
          <span
            key={i}
            className="inline-flex items-center gap-0.5 px-1.5 py-0.5 text-[10px] rounded bg-slate-100 text-slate-600 max-w-[180px] truncate"
            title={`${e.field}: ${e.value}${e.reason ? ` (${e.reason})` : ''}`}
          >
            <span className="font-semibold text-slate-500">{e.field}:</span>
            <span className="truncate">{displayValue}</span>
          </span>
        )
      })}
      {remaining > 0 && (
        <span
          className="inline-flex items-center px-1.5 py-0.5 text-[10px] rounded bg-slate-50 text-slate-400"
          title={evidence.slice(3).map(e => `${e.field}: ${e.value}`).join('\n')}
        >
          +{remaining} more
        </span>
      )}
    </div>
  )
})

const FilterSelect = memo(function FilterSelect({
  value,
  onChange,
  options,
}: {
  value: string
  onChange: (v: string) => void
  options: readonly { value: string; labelKey: string }[]
}) {
  const { t } = useTranslation()
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="text-xs px-2 py-1 rounded-lg border border-slate-200 bg-white text-slate-600
                 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-300
                 cursor-pointer hover:border-slate-300 transition-colors"
    >
      {options.map((opt) => (
        <option key={opt.value} value={opt.value}>{t(opt.labelKey)}</option>
      ))}
    </select>
  )
})

// ─── Main Component ──────────────────────────────────────────────────────────

export const TrafficTransactionsTable = memo(function TrafficTransactionsTable() {
  const { t } = useTranslation()
  const [trafficFilter, setTrafficFilter] = useState('')
  const [platformFilter, setPlatformFilter] = useState('')
  const [offset, setOffset] = useState(0)
  // Accumulate loaded pages; keyed by the filter context to reset on change
  const pagesRef = useRef<TrafficTransaction[]>([])
  const prevKeyRef = useRef('')

  const queryParams = useQueryParams()
  const filterValue = trafficFilter || null
  const platformValue = platformFilter || null

  // Build a stable key from all filters that should reset pagination
  const resetKey = `${queryParams}|${trafficFilter}|${platformFilter}`

  // Reset accumulated pages when any filter changes
  if (resetKey !== prevKeyRef.current) {
    prevKeyRef.current = resetKey
    pagesRef.current = []
    // Reset offset synchronously to avoid stale fetch
    if (offset !== 0) {
      setOffset(0)
    }
  }

  const { data, isLoading, error, refetch } = useTrafficTransactions(filterValue, platformValue, PAGE_SIZE, offset)

  // Build the full visible list from accumulated pages + current data
  const allTransactions = useMemo(() => {
    if (!data?.transactions) return pagesRef.current

    if (offset === 0) {
      // First page - replace
      pagesRef.current = data.transactions
    } else if (pagesRef.current.length <= offset) {
      // Append new page (guard against double-append)
      pagesRef.current = [...pagesRef.current, ...data.transactions]
    }

    return pagesRef.current
  }, [data, offset])

  const handleFilterChange = useCallback((value: string) => {
    setTrafficFilter(value)
    setOffset(0)
  }, [])

  const handlePlatformChange = useCallback((value: string) => {
    setPlatformFilter(value)
    setOffset(0)
  }, [])

  const handleLoadMore = useCallback(() => {
    setOffset(prev => prev + PAGE_SIZE)
  }, [])

  const total = data?.total ?? 0
  const hasMore = allTransactions.length < total
  const isEmpty = !isLoading && allTransactions.length === 0

  return (
    <ChartContainer
      title={t('traffic.orderDetails')}
      titleExtra={
        <div className="flex items-center gap-2">
          <FilterSelect value={trafficFilter} onChange={handleFilterChange} options={TRAFFIC_TYPES} />
          <FilterSelect value={platformFilter} onChange={handlePlatformChange} options={PLATFORMS} />
        </div>
      }
      isLoading={isLoading && offset === 0}
      error={error as Error | null}
      onRetry={refetch}
      height="auto"
      isEmpty={isEmpty}
      emptyMessage={t('traffic.noOrdersData')}
      ariaLabel={t('traffic.orderDetailsDesc')}
    >
      <div className="overflow-auto rounded-lg border border-slate-200 max-h-[500px]">
        <table className="w-full text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="bg-slate-50 border-b border-slate-200">
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                {t('traffic.orderNum')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden md:table-cell">
                {t('common.date')}
              </th>
              <th className="text-right py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                {t('common.amount')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden md:table-cell">
                {t('traffic.source')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden md:table-cell">
                {t('traffic.platform')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                {t('chart.type')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden md:table-cell">
                {t('traffic.campaign')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden lg:table-cell">
                {t('traffic.evidence')}
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {allTransactions.map((tx) => (
              <tr key={tx.id} className="hover:bg-slate-50 transition-colors">
                <td className="py-3 px-4 font-mono text-slate-800 text-xs">
                  {tx.id}
                </td>
                <td className="py-3 px-4 text-slate-600 whitespace-nowrap hidden md:table-cell">
                  {new Date(tx.date).toLocaleDateString('uk-UA', {
                    day: '2-digit',
                    month: '2-digit',
                    year: 'numeric',
                  })}
                </td>
                <td className="py-3 px-4 text-right text-slate-800 font-semibold whitespace-nowrap">
                  {formatCurrency(tx.amount)}
                </td>
                <td className="py-3 px-4 text-slate-600 hidden md:table-cell">
                  {tx.source}
                </td>
                <td className="py-3 px-4 text-slate-600 capitalize hidden md:table-cell">
                  {tx.platform}
                </td>
                <td className="py-3 px-4">
                  <TrafficBadge type={tx.traffic_type} />
                </td>
                <td className="py-3 px-4 hidden md:table-cell">
                  <span className="text-xs text-slate-500 max-w-[200px] truncate block" title={tx.evidence.find(e => e.field === 'utm_campaign')?.value}>
                    {tx.evidence.find(e => e.field === 'utm_campaign')?.value || '--'}
                  </span>
                </td>
                <td className="py-3 px-4 hidden lg:table-cell">
                  <EvidencePills evidence={tx.evidence} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Footer: count + load more */}
      <div className="flex items-center justify-between mt-3 px-1">
        <span className="text-xs text-slate-400">
          {t('traffic.showing')} {allTransactions.length} {t('common.of')} {total}
        </span>
        {hasMore && (
          <button
            onClick={handleLoadMore}
            disabled={isLoading}
            className="text-xs px-3 py-1.5 rounded-lg bg-slate-100 text-slate-600 hover:bg-slate-200
                       transition-colors disabled:opacity-50 disabled:cursor-not-allowed font-medium"
          >
            {isLoading ? t('common.loading') : t('traffic.loadMore')}
          </button>
        )}
      </div>
    </ChartContainer>
  )
})
