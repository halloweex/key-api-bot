import { type ReactNode, useCallback, memo } from 'react'
import { Card, CardHeader, CardTitle, CardContent, SkeletonChart } from '../ui'
import { CHART_DIMENSIONS, type ChartHeight } from './config'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartContainerProps {
  /** Chart title displayed in header */
  title: string
  /** Chart content */
  children: ReactNode
  /** Loading state */
  isLoading?: boolean
  /** Error object */
  error?: Error | null
  /** Retry function for error recovery */
  onRetry?: () => void
  /** Additional CSS classes */
  className?: string
  /** Action button/element in header */
  action?: ReactNode
  /** Chart height preset */
  height?: ChartHeight
  /** Accessible description */
  ariaLabel?: string
  /** Show when data is empty */
  isEmpty?: boolean
  /** Custom empty state message */
  emptyMessage?: string
}

// ─── Subcomponents ───────────────────────────────────────────────────────────

interface ErrorStateProps {
  title: string
  onRetry?: () => void
  height: number
}

const ErrorState = memo(function ErrorState({ title, onRetry, height }: ErrorStateProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <div
          role="alert"
          className="flex flex-col items-center justify-center gap-4"
          style={{ height }}
        >
          <div className="w-12 h-12 rounded-full bg-red-50 flex items-center justify-center">
            <svg
              className="w-6 h-6 text-red-500"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              aria-hidden="true"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={1.5}
                d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
          </div>
          <div className="text-center">
            <p className="text-slate-700 text-sm font-medium">Failed to load data</p>
            <p className="text-slate-500 text-xs mt-1">Please try again later</p>
          </div>
          {onRetry && (
            <button
              onClick={onRetry}
              className="text-sm text-blue-600 hover:text-blue-700 font-medium px-4 py-2 rounded-lg hover:bg-blue-50 transition-colors"
            >
              Try again
            </button>
          )}
        </div>
      </CardContent>
    </Card>
  )
})

interface EmptyStateProps {
  message: string
  height: number
}

const EmptyState = memo(function EmptyState({ message, height }: EmptyStateProps) {
  return (
    <div
      className="flex flex-col items-center justify-center"
      style={{ height }}
    >
      <div className="w-12 h-12 rounded-full bg-slate-100 flex items-center justify-center mb-3">
        <svg
          className="w-6 h-6 text-slate-400"
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
          aria-hidden="true"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={1.5}
            d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z"
          />
        </svg>
      </div>
      <p className="text-sm text-slate-500 font-medium">{message}</p>
      <p className="text-xs text-slate-400 mt-1">Try adjusting your filters</p>
    </div>
  )
})

// ─── Main Component ──────────────────────────────────────────────────────────

export const ChartContainer = memo(function ChartContainer({
  title,
  children,
  isLoading = false,
  error = null,
  onRetry,
  className = '',
  action,
  height = 'lg',
  ariaLabel,
  isEmpty = false,
  emptyMessage = 'No data available',
}: ChartContainerProps) {
  const chartHeight = CHART_DIMENSIONS.height[height]

  const handleRetry = useCallback(() => {
    onRetry?.()
  }, [onRetry])

  // Loading state
  if (isLoading) {
    return <SkeletonChart />
  }

  // Error state with retry
  if (error) {
    return (
      <ErrorState
        title={title}
        onRetry={onRetry ? handleRetry : undefined}
        height={chartHeight}
      />
    )
  }

  return (
    <Card className={className}>
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle>{title}</CardTitle>
        {action}
      </CardHeader>
      <CardContent>
        <figure
          role="img"
          aria-label={ariaLabel || `${title} chart`}
        >
          {isEmpty ? (
            <EmptyState message={emptyMessage} height={chartHeight} />
          ) : (
            children
          )}
        </figure>
      </CardContent>
    </Card>
  )
})

// ─── Re-export for convenience ───────────────────────────────────────────────

export { CHART_DIMENSIONS, CHART_THEME } from './config'
