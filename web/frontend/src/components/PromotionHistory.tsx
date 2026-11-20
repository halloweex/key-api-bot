import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { Button } from './Button'
import { Wrapper } from './Wrapper'
import { describeHistory } from '../utils/promotionDiff'
import type { PromotionVersion } from '../types/api'

// ─── PromotionHistory ────────────────────────────────────────────────────────
//
// «Історія змін»: a promotion's saved versions, newest first, each said as
// what changed since the one before it (`utils/promotionDiff`) — «Перенесено:
// 5–8 → 6–9 березня», «Знижка: 20 → 25 %», «Скасовано». Who saved a version
// is not said; only that it was the reader, which the API tells and nothing
// more (`recorded_by_me`).

interface PromotionHistoryProps {
  versions: PromotionVersion[] | undefined
  isLoading: boolean
  error: Error | null
  onRetry: () => void
}

export const PromotionHistory = memo(function PromotionHistory({
  versions,
  isLoading,
  error,
  onRetry,
}: PromotionHistoryProps) {
  const { t } = useTranslation()

  if (isLoading) {
    return <p className="text-xs text-slate-500" role="status">{t('common.loading')}</p>
  }
  if (error) {
    return (
      <Wrapper dir="row" gap="sm" align="center" wrap>
        <p className="text-xs text-red-600">{t('promo.historyFailed')}</p>
        <Button size="sm" variant="ghost" onClick={onRetry}>{t('error.retry')}</Button>
      </Wrapper>
    )
  }

  const entries = describeHistory(versions ?? [], t)
  return (
    <ol className="border-l-2 border-slate-200 pl-3" aria-label={t('promo.history')}>
      {entries.map((entry) => (
        <li key={entry.version} className="py-1.5">
          <p className="text-[11px] text-slate-500 tabular-nums">
            {entry.when}
            {entry.byMe && ` · ${t('promo.byYou')}`}
          </p>
          <ul className="text-xs text-slate-700">
            {entry.changes.map((line, i) => <li key={i}>{line}</li>)}
          </ul>
        </li>
      ))}
    </ol>
  )
})
