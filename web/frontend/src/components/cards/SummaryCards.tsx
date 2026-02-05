import { useMemo, useCallback, useState, useRef, useEffect, type ReactNode } from 'react'
import { StatCard, StatCardSkeleton, type StatCardVariant } from './StatCard'
import { MilestoneProgress } from '../ui'
import { useSummary, useReturns } from '../../hooks'
import { formatCurrency, formatNumber, formatPercent } from '../../utils/formatters'
import type { SummaryResponse, ReturnOrder } from '../../types/api'
import {
  ShoppingCartIcon,
  CurrencyIcon,
  CalculatorIcon,
  ArrowUturnLeftIcon,
} from '../icons'

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
]

const SKELETON_COUNT = CARD_CONFIGS.length + 1 // +1 for returns card

// ─── Status Badge Colors ─────────────────────────────────────────────────────

const STATUS_COLORS: Record<number, string> = {
  19: 'bg-red-100 text-red-700',      // Returned
  21: 'bg-orange-100 text-orange-700', // Partially Returned
  22: 'bg-slate-100 text-slate-700',   // Cancelled
  23: 'bg-purple-100 text-purple-700', // Refunded
}

// ─── Returns Card with Dropdown ──────────────────────────────────────────────

interface ReturnsCardProps {
  data: SummaryResponse
}

function ReturnsCard({ data }: ReturnsCardProps) {
  const [isOpen, setIsOpen] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const { data: returnsData, isLoading } = useReturns(isOpen)

  const returnRate = data.totalOrders > 0
    ? (data.totalReturns / (data.totalOrders + data.totalReturns)) * 100
    : 0

  // Close on click outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false)
      }
    }
    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside)
    }
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [isOpen])

  // Close on escape
  useEffect(() => {
    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setIsOpen(false)
    }
    if (isOpen) {
      document.addEventListener('keydown', handleEscape)
    }
    return () => document.removeEventListener('keydown', handleEscape)
  }, [isOpen])

  const handleClick = useCallback(() => {
    if (data.totalReturns > 0) {
      setIsOpen((prev) => !prev)
    }
  }, [data.totalReturns])

  return (
    <div ref={dropdownRef} className="relative">
      <button
        onClick={handleClick}
        disabled={data.totalReturns === 0}
        className={`w-full text-left transition-all ${
          data.totalReturns > 0
            ? 'cursor-pointer hover:ring-2 hover:ring-orange-300 rounded-xl'
            : 'cursor-default'
        }`}
        aria-expanded={isOpen}
        aria-haspopup="true"
      >
        <StatCard
          label="Returns"
          value={data.totalReturns}
          formatter={formatNumber}
          variant="orange"
          icon={<ArrowUturnLeftIcon />}
          subtitle={returnRate > 0 ? `${formatPercent(returnRate)} return rate` : undefined}
          ariaLabel={`Returns: ${formatNumber(data.totalReturns)}`}
          clickable={data.totalReturns > 0}
        />
      </button>

      {/* Dropdown */}
      {isOpen && (
        <div className="absolute top-full left-0 right-0 mt-2 z-50 bg-white rounded-xl shadow-xl border border-slate-200 overflow-hidden max-h-80 overflow-y-auto">
          <div className="sticky top-0 bg-white border-b border-slate-100 px-4 py-2">
            <div className="flex items-center justify-between">
              <span className="text-sm font-medium text-slate-700">
                Return Orders ({data.totalReturns})
              </span>
              <button
                onClick={() => setIsOpen(false)}
                className="text-slate-400 hover:text-slate-600 p-1"
              >
                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                </svg>
              </button>
            </div>
          </div>

          {isLoading ? (
            <div className="p-4 text-center text-slate-500 text-sm">
              Loading returns...
            </div>
          ) : returnsData?.returns?.length ? (
            <div className="divide-y divide-slate-100">
              {returnsData.returns.map((order: ReturnOrder) => (
                <div key={order.id} className="px-4 py-3 hover:bg-slate-50 transition-colors">
                  <div className="flex items-center justify-between gap-3">
                    <div className="min-w-0 flex-1">
                      <div className="flex items-center gap-2">
                        <span className="font-medium text-slate-800">#{order.id}</span>
                        <span className={`text-xs px-2 py-0.5 rounded-full ${STATUS_COLORS[order.statusId] || 'bg-slate-100 text-slate-600'}`}>
                          {order.statusName}
                        </span>
                      </div>
                      <div className="text-xs text-slate-500 mt-0.5">
                        {order.date} · {order.source}
                      </div>
                    </div>
                    <div className="text-right">
                      <div className="font-medium text-slate-800">
                        {formatCurrency(order.amount)}
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="p-4 text-center text-slate-500 text-sm">
              No return orders found
            </div>
          )}
        </div>
      )}
    </div>
  )
}

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
        className="grid grid-cols-2 lg:grid-cols-4 gap-2 sm:gap-4 mobile-single-col"
      >
        {isLoading && <LoadingState />}

        {error && !isLoading && (
          <ErrorState
            message="Failed to load summary data"
            onRetry={handleRetry}
          />
        )}

        {!isLoading && !error && data && (
          <>
            {renderedCards}
            <ReturnsCard data={data} />
          </>
        )}
      </section>
    </div>
  )
}
