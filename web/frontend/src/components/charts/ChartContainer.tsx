import { type ReactNode, useCallback, memo } from 'react'
import { useTranslation } from 'react-i18next'
import { Card, CardHeader, CardTitle, CardContent, SkeletonChart, ApiErrorState } from '../ui'
import { LottieAnimation } from '../ui/LottieAnimation'
import emptyDataAnimation from '../../assets/animations/empty-data.json'
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
  const { t } = useTranslation()
  return (
    <Card>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
      </CardHeader>
      <CardContent>
        <ApiErrorState
          error={error}
          onRetry={onRetry}
          title={`${t('chart.failedToLoad')} ${title.toLowerCase()}`}
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
  const { t } = useTranslation()
  return (
    <div
      className="flex flex-col items-center justify-center animate-fade-in"
      style={{ height }}
    >
      <LottieAnimation
        animationData={emptyDataAnimation}
        className="w-24 h-24 mb-2"
      />
      <p className="text-sm text-slate-600 font-medium">{message}</p>
      <p className="text-xs text-slate-400 mt-1.5">{t('chart.adjustFilters')}</p>
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
  emptyMessage,
}: ChartContainerProps) {
  const { t } = useTranslation()
  const resolvedEmptyMessage = emptyMessage || t('chart.noData')
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
            <EmptyState message={resolvedEmptyMessage} height={chartHeight} />
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
