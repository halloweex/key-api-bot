import { useMemo, useCallback } from 'react'
import { StatCard, StatCardSkeleton, type StatCardVariant } from './StatCard'
import { MilestoneProgress } from '../ui'
import { useSummary } from '../../hooks'
import { formatCurrency, formatNumber, formatPercent } from '../../utils/formatters'
import type { SummaryResponse } from '../../types/api'

// ─── Types ───────────────────────────────────────────────────────────────────

interface CardConfig {
  id: string
  label: string
  variant: StatCardVariant
  getValue: (data: SummaryResponse) => number
  formatter: (value: number) => string
  getSubtitle?: (data: SummaryResponse) => string | undefined
  ariaLabel?: (data: SummaryResponse) => string
}

// ─── Card Configuration ──────────────────────────────────────────────────────

const CARD_CONFIGS: CardConfig[] = [
  {
    id: 'orders',
    label: 'Total Orders',
    variant: 'blue',
    getValue: (data) => data.totalOrders,
    formatter: formatNumber,
    getSubtitle: (data) => `${data.startDate} - ${data.endDate}`,
    ariaLabel: (data) => `Total orders: ${formatNumber(data.totalOrders)} from ${data.startDate} to ${data.endDate}`,
  },
  {
    id: 'revenue',
    label: 'Total Revenue',
    variant: 'green',
    getValue: (data) => data.totalRevenue,
    formatter: formatCurrency,
    ariaLabel: (data) => `Total revenue: ${formatCurrency(data.totalRevenue)}`,
  },
  {
    id: 'avgCheck',
    label: 'Average Check',
    variant: 'purple',
    getValue: (data) => data.avgCheck,
    formatter: formatCurrency,
    ariaLabel: (data) => `Average check: ${formatCurrency(data.avgCheck)}`,
  },
  {
    id: 'returns',
    label: 'Returns',
    variant: 'orange',
    getValue: (data) => data.totalReturns,
    formatter: formatNumber,
    getSubtitle: (data) => {
      const rate = data.totalOrders > 0
        ? (data.totalReturns / (data.totalOrders + data.totalReturns)) * 100
        : 0
      return rate > 0 ? `${formatPercent(rate)} return rate` : undefined
    },
    ariaLabel: (data) => `Returns: ${formatNumber(data.totalReturns)}`,
  },
]

const SKELETON_COUNT = CARD_CONFIGS.length

// ─── Error Component ─────────────────────────────────────────────────────────

interface ErrorStateProps {
  message: string
  onRetry?: () => void
}

function ErrorState({ message, onRetry }: ErrorStateProps) {
  return (
    <div
      role="alert"
      className="col-span-full bg-red-900/20 border border-red-800 rounded-lg p-4"
    >
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          <svg
            className="w-5 h-5 text-red-400"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
            aria-hidden="true"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
            />
          </svg>
          <p className="text-red-300 text-sm">{message}</p>
        </div>
        {onRetry && (
          <button
            onClick={onRetry}
            className="text-sm text-red-300 hover:text-red-200 underline underline-offset-2 transition-colors"
          >
            Retry
          </button>
        )}
      </div>
    </div>
  )
}

// ─── Loading State ───────────────────────────────────────────────────────────

function LoadingState() {
  return (
    <>
      {Array.from({ length: SKELETON_COUNT }, (_, i) => (
        <StatCardSkeleton key={`skeleton-${i}`} />
      ))}
    </>
  )
}

// ─── Main Component ──────────────────────────────────────────────────────────

export function SummaryCards() {
  const { data, isLoading, error, refetch } = useSummary()

  const handleRetry = useCallback(() => {
    refetch()
  }, [refetch])

  // Memoize rendered cards to prevent unnecessary re-renders
  const renderedCards = useMemo(() => {
    if (!data) return null

    return CARD_CONFIGS.map((config) => (
      <StatCard
        key={config.id}
        label={config.label}
        value={config.getValue(data)}
        formatter={config.formatter}
        variant={config.variant}
        subtitle={config.getSubtitle?.(data)}
        ariaLabel={config.ariaLabel?.(data)}
      />
    ))
  }, [data])

  return (
    <div className="space-y-4">
      {/* Milestone Progress Bar */}
      {!isLoading && !error && data && (
        <MilestoneProgress revenue={data.totalRevenue} />
      )}

      {/* Summary Cards Grid */}
      <section
        aria-label="Summary statistics"
        className="grid grid-cols-2 lg:grid-cols-4 gap-4"
      >
        {isLoading && <LoadingState />}

        {error && !isLoading && (
          <ErrorState
            message="Failed to load summary data"
            onRetry={handleRetry}
          />
        )}

        {!isLoading && !error && data && renderedCards}
      </section>
    </div>
  )
}
