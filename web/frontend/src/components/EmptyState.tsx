import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { LottieAnimation } from './LottieAnimation'
import { Wrapper } from './Wrapper'
import emptyDataAnimation from '../assets/animations/empty-data.json'

// ─── EmptyState ──────────────────────────────────────────────────────────────
//
// Shared empty-state scene used by both ChartContainer (chart panels) and
// page-level tables (Admin, Reports). Visual is owned here; consumers pass
// only `message` and optionally a `hint` line below it.

interface EmptyStateProps {
  message: string
  /** Secondary helper line — defaults to "Adjust filters and try again". */
  hint?: string
}

export const EmptyState = memo(function EmptyState({ message, hint }: EmptyStateProps) {
  const { t } = useTranslation()
  return (
    <div className="h-full flex flex-col items-center justify-center py-10 animate-fade-in">
      <Wrapper marginBottom="sm">
        <LottieAnimation animationData={emptyDataAnimation} size="lg" />
      </Wrapper>
      <p className="text-sm text-slate-600 font-medium">{message}</p>
      <p className="text-xs text-slate-400 mt-1.5">{hint ?? t('chart.adjustFilters')}</p>
    </div>
  )
})
