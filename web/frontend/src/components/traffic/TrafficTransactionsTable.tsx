import { memo, useState, useCallback, useRef, useMemo } from 'react'
import { ChartContainer } from '../charts/ChartContainer'
import { formatCurrency } from '../../utils/formatters'
import { useTrafficTransactions } from '../../hooks'
import { useQueryParams } from '../../store/filterStore'
import type { TrafficEvidence, TrafficTransaction } from '../../types/api'

// ─── Traffic Type Config ─────────────────────────────────────────────────────

const TRAFFIC_TYPES = [
  { value: '', label: 'All types' },
  { value: 'paid_confirmed', label: 'Paid' },
  { value: 'paid_likely', label: 'Paid (likely)' },
  { value: 'manager', label: 'Sales Manager' },
  { value: 'organic', label: 'Organic' },
  { value: 'pixel_only', label: 'Pixel Only' },
  { value: 'unknown', label: 'Unknown' },
] as const

const trafficBadgeConfig: Record<string, { bg: string; text: string; label: string }> = {
  paid_confirmed: { bg: 'bg-blue-100', text: 'text-blue-700', label: 'Paid' },
  paid_likely: { bg: 'bg-blue-50', text: 'text-blue-500', label: 'Paid (likely)' },
  manager: { bg: 'bg-cyan-100', text: 'text-cyan-700', label: 'Manager' },
  organic: { bg: 'bg-green-100', text: 'text-green-700', label: 'Organic' },
  pixel_only: { bg: 'bg-orange-100', text: 'text-orange-700', label: 'Pixel Only' },
  unknown: { bg: 'bg-purple-100', text: 'text-purple-700', label: 'Unknown' },
}

const PAGE_SIZE = 50

// ─── Sub-components ──────────────────────────────────────────────────────────

const TrafficBadge = memo(function TrafficBadge({ type }: { type: string }) {
  const config = trafficBadgeConfig[type] || trafficBadgeConfig.unknown
  return (
    <span className={`inline-flex px-2 py-0.5 text-xs font-medium rounded-full ${config.bg} ${config.text}`}>
      {config.label}
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

const TrafficTypeFilter = memo(function TrafficTypeFilter({
  value,
  onChange,
}: {
  value: string
  onChange: (v: string) => void
}) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className="text-xs px-2 py-1 rounded-lg border border-slate-200 bg-white text-slate-600
                 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-300
                 cursor-pointer hover:border-slate-300 transition-colors"
    >
      {TRAFFIC_TYPES.map((t) => (
        <option key={t.value} value={t.value}>{t.label}</option>
      ))}
    </select>
  )
})

// ─── Main Component ──────────────────────────────────────────────────────────

export const TrafficTransactionsTable = memo(function TrafficTransactionsTable() {
  const [trafficFilter, setTrafficFilter] = useState('')
  const [offset, setOffset] = useState(0)
  // Accumulate loaded pages; keyed by the filter context to reset on change
  const pagesRef = useRef<TrafficTransaction[]>([])
  const prevKeyRef = useRef('')

  const queryParams = useQueryParams()
  const filterValue = trafficFilter || null

  // Build a stable key from all filters that should reset pagination
  const resetKey = `${queryParams}|${trafficFilter}`

  // Reset accumulated pages when any filter changes
  if (resetKey !== prevKeyRef.current) {
    prevKeyRef.current = resetKey
    pagesRef.current = []
    // Reset offset synchronously to avoid stale fetch
    if (offset !== 0) {
      setOffset(0)
    }
  }

  const { data, isLoading, error, refetch } = useTrafficTransactions(filterValue, PAGE_SIZE, offset)

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

  const handleLoadMore = useCallback(() => {
    setOffset(prev => prev + PAGE_SIZE)
  }, [])

  const total = data?.total ?? 0
  const hasMore = allTransactions.length < total
  const isEmpty = !isLoading && allTransactions.length === 0

  return (
    <ChartContainer
      title="Order Details"
      titleExtra={
        <TrafficTypeFilter value={trafficFilter} onChange={handleFilterChange} />
      }
      isLoading={isLoading && offset === 0}
      error={error as Error | null}
      onRetry={refetch}
      height="auto"
      isEmpty={isEmpty}
      emptyMessage="No orders with traffic data for this period"
      ariaLabel="Table showing individual orders with traffic attribution"
    >
      <div className="overflow-auto rounded-lg border border-slate-200 max-h-[500px]">
        <table className="w-full text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="bg-slate-50 border-b border-slate-200">
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                Order #
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden md:table-cell">
                Date
              </th>
              <th className="text-right py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                Amount
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden md:table-cell">
                Source
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden md:table-cell">
                Platform
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                Type
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden lg:table-cell">
                Evidence
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
          Showing {allTransactions.length} of {total}
        </span>
        {hasMore && (
          <button
            onClick={handleLoadMore}
            disabled={isLoading}
            className="text-xs px-3 py-1.5 rounded-lg bg-slate-100 text-slate-600 hover:bg-slate-200
                       transition-colors disabled:opacity-50 disabled:cursor-not-allowed font-medium"
          >
            {isLoading ? 'Loading...' : 'Load more'}
          </button>
        )}
      </div>
    </ChartContainer>
  )
})
