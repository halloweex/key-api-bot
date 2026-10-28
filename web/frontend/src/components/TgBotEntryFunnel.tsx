import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from './ChartContainer'
import { formatNumber } from '../utils/formatters'
import type { TgBotEntryStep } from '../types/api'

// How far the people first seen in the window got, step by step. The share
// beside each bar is of the step above it: that is where the drop is.

interface TgBotEntryFunnelProps {
  steps: TgBotEntryStep[]
  isLoading?: boolean
  error?: Error | null
  onRetry?: () => void
}

export const TgBotEntryFunnel = memo(function TgBotEntryFunnel({
  steps, isLoading, error, onRetry,
}: TgBotEntryFunnelProps) {
  const { t } = useTranslation()
  const top = steps[0]?.people ?? 0

  return (
    <ChartContainer
      title={t('tgBot.entryTitle')}
      isLoading={isLoading}
      error={error ?? null}
      onRetry={onRetry}
      isEmpty={!isLoading && top === 0}
      emptyMessage={t('tgBot.entryEmpty')}
      height="auto"
      ariaLabel={t('tgBot.entryTitle')}
    >
      <ol className="space-y-3">
        {steps.map((step, i) => {
          const previous = i === 0 ? null : steps[i - 1].people
          const ofPrevious = previous ? Math.round((100 * step.people) / previous) : null
          const width = top ? Math.max(2, (100 * step.people) / top) : 0
          return (
            <li key={step.key}>
              <div className="flex items-baseline justify-between gap-3 text-sm">
                <span className="text-slate-700">{t(`tgBot.step.${step.key}`)}</span>
                <span className="tabular-nums text-slate-900 font-medium whitespace-nowrap">
                  {formatNumber(step.people)}
                  {ofPrevious !== null && (
                    <span className={`ml-2 text-xs ${ofPrevious < 50 ? 'text-rose-600' : 'text-slate-500'}`}>
                      {ofPrevious}%
                    </span>
                  )}
                </span>
              </div>
              <div className="mt-1 h-2 rounded-full bg-slate-100">
                <div className="h-2 rounded-full bg-blue-600" style={{ width: `${width}%` }} />
              </div>
            </li>
          )
        })}
      </ol>
      <p className="mt-4 text-xs text-slate-500">{t('tgBot.entryHint')}</p>
    </ChartContainer>
  )
})

export default TgBotEntryFunnel
