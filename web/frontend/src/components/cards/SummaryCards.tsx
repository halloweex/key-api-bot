import { useMemo, useCallback, type ReactNode } from 'react'
import { StatCard, StatCardSkeleton, type StatCardVariant } from './StatCard'
import { MilestoneProgress } from '../ui'
import { useSummary } from '../../hooks'
import { formatCurrency, formatNumber, formatPercent } from '../../utils/formatters'
import type { SummaryResponse } from '../../types/api'

// ─── Icons ────────────────────────────────────────────────────────────────────

const ShoppingCartIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M3 3h2l.4 2M7 13h10l4-8H5.4M7 13L5.4 5M7 13l-2.293 2.293c-.63.63-.184 1.707.707 1.707H17m0 0a2 2 0 100 4 2 2 0 000-4zm-8 2a2 2 0 11-4 0 2 2 0 014 0z" />
  </svg>
)

const CurrencyIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
  </svg>
)

const CalculatorIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M9 7h6m0 10v-3m-3 3h.01M9 17h.01M9 14h.01M12 14h.01M15 11h.01M12 11h.01M9 11h.01M7 21h10a2 2 0 002-2V5a2 2 0 00-2-2H7a2 2 0 00-2 2v14a2 2 0 002 2z" />
  </svg>
)

const ArrowUturnLeftIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M9 15L3 9m0 0l6-6M3 9h12a6 6 0 010 12h-3" />
  </svg>
)

// ─── Types ───────────────────────────────────────────────────────────────────

interface CardConfig {
  id: string
  label: string
  variant: StatCardVariant
  icon: ReactNode
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
    icon: <ShoppingCartIcon />,
    getValue: (data) => data.totalOrders,
    formatter: formatNumber,
    getSubtitle: (data) => `${data.startDate} - ${data.endDate}`,
    ariaLabel: (data) => `Total orders: ${formatNumber(data.totalOrders)} from ${data.startDate} to ${data.endDate}`,
  },
  {
    id: 'revenue',
    label: 'Total Revenue',
    variant: 'green',
    icon: <CurrencyIcon />,
    getValue: (data) => data.totalRevenue,
    formatter: formatCurrency,
    ariaLabel: (data) => `Total revenue: ${formatCurrency(data.totalRevenue)}`,
  },
  {
    id: 'avgCheck',
    label: 'Average Check',
    variant: 'purple',
    icon: <CalculatorIcon />,
    getValue: (data) => data.avgCheck,
    formatter: formatCurrency,
    ariaLabel: (data) => `Average check: ${formatCurrency(data.avgCheck)}`,
  },
  {
    id: 'returns',
    label: 'Returns',
    variant: 'orange',
    icon: <ArrowUturnLeftIcon />,
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
      className="col-span-full bg-red-50 border border-red-200 rounded-lg p-4"
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
          <p className="text-red-700 text-sm">{message}</p>
        </div>
        {onRetry && (
          <button
            onClick={onRetry}
            className="text-sm text-red-600 hover:text-red-800 underline underline-offset-2 transition-colors"
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
        icon={config.icon}
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
