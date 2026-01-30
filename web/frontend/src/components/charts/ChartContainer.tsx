import { type ReactNode, useCallback, memo } from 'react'
import { Card, CardHeader, CardTitle, CardContent, SkeletonChart, ApiErrorState } from '../ui'
import { CHART_DIMENSIONS, type ChartHeight } from './config'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartContainerProps {
  /** Chart title displayed in header */
  title: string
  /** Extra element rendered next to title (e.g. info button) */
  titleExtra?: ReactNode
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
  /** Chart height preset or 'auto' for dynamic height */
  height?: ChartHeight | 'auto'
  /** Accessible description */
  ariaLabel?: string
  /** Show when data is empty */
  isEmpty?: boolean
  /** Custom empty state message */
  emptyMessage?: string
}

// ─── Subcomponents ───────────────────────────────────────────────────────────

interface ErrorStateWrapperProps {
  title: string
  error: Error | null
  onRetry?: () => void
}

const ErrorStateWrapper = memo(function ErrorStateWrapper({
  title,
  error,
  onRetry
}: ErrorStateWrapperProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <ApiErrorState
          error={error}
          onRetry={onRetry}
          title={`Failed to load ${title.toLowerCase()}`}
        />
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
  titleExtra,
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
  const chartHeight = height === 'auto' ? CHART_DIMENSIONS.height.lg : CHART_DIMENSIONS.height[height]

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
      <ErrorStateWrapper
        title={title}
        error={error}
        onRetry={onRetry ? handleRetry : undefined}
      />
    )
  }

  return (
    <Card className={`animate-chart-in ${className}`}>
      <CardHeader className="flex flex-row items-center gap-1.5">
        <div className="flex items-center gap-1">
          <CardTitle>{title}</CardTitle>
          {titleExtra}
        </div>
        {action}
      </CardHeader>
      <CardContent>
        <figure
          role="img"
          aria-label={ariaLabel || `${title} chart`}
          className="animate-chart-scale"
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
