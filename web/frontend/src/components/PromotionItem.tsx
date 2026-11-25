import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Badge } from './Badge'
import { Button } from './Button'
import { PromotionHistory } from './PromotionHistory'
import { Wrapper } from './Wrapper'
import { usePromotionVersions } from '../hooks/useApi'
import { formatDiscount, isOngoing, promotionDays } from '../utils/promotions'
import type { Promotion } from '../types/api'

// ─── PromotionItem ───────────────────────────────────────────────────────────
//
// One promotion in the calendar's lists: its name and type, then one line of
// what was recorded — the days and the hour it opens, how deep the discount
// goes, on what, where it is announced. With `marketing` edit it can be
// changed, cancelled or restored; every change is a new version, and once
// there is more than one, «Історія змін» says them in words.

/** How many names of a scope are said before «ще N». */
const ITEMS_SHOWN = 3

interface PromotionItemProps {
  promotion: Promotion
  /** Kyiv today, `YYYY-MM-DD`. */
  today: string
  /** `marketing` edit: «Змінити», «Скасувати», «Відновити». */
  canEdit: boolean
  /** Its cancel or restore is on its way. */
  busy?: boolean
  onEdit: (promotion: Promotion) => void
  onCancel: (promotion: Promotion) => void
  onRestore: (promotion: Promotion) => void
}

export const PromotionItem = memo(function PromotionItem({
  promotion: p,
  today,
  canEdit,
  busy = false,
  onEdit,
  onCancel,
  onRestore,
}: PromotionItemProps) {
  const { t } = useTranslation()
  const [historyOpen, setHistoryOpen] = useState(false)
  const history = usePromotionVersions(p.id, historyOpen)
  const cancelled = p.status === 'cancelled'

  const items = p.scope_items.length > ITEMS_SHOWN
    ? `${p.scope_items.slice(0, ITEMS_SHOWN).join(', ')} ${t('promo.moreItems', { count: p.scope_items.length - ITEMS_SHOWN })}`
    : p.scope_items.join(', ')
  const details = [
    p.start_time
      ? `${promotionDays(p.starts_on, p.ends_on, today)}, ${t('promo.startsAt', { time: p.start_time.slice(0, 5) })}`
      : promotionDays(p.starts_on, p.ends_on, today),
    p.discount_pct !== null && p.discount_pct !== undefined
      ? t('promo.discountUpTo', { pct: formatDiscount(p.discount_pct) })
      : null,
    p.scope === 'store' || !items
      ? t(`promo.scope.${p.scope}`)
      : t('promo.scopeWithItems', { scope: t(`promo.scope.${p.scope}`), items }),
    p.channels.length ? p.channels.map((c) => t(`promo.channel.${c}`)).join(', ') : null,
  ].filter(Boolean)

  return (
    <li className="py-3 first:pt-0 last:pb-0">
      <Wrapper dir="column" gap="xs">
        <Wrapper dir="row" gap="sm" align="center" wrap>
          <span className={`text-sm font-semibold ${cancelled ? 'text-slate-400 line-through' : 'text-slate-800'}`}>
            {p.name}
          </span>
          <Badge tone={p.kind === 'black_friday' ? 'indigo' : 'slate'}>{t(`promo.kind.${p.kind}`)}</Badge>
          {cancelled && <Badge tone="red">{t('promo.statusCancelled')}</Badge>}
          {!cancelled && isOngoing(p, today) && <Badge tone="green">{t('promo.onNow')}</Badge>}
        </Wrapper>
        <p className={`text-xs ${cancelled ? 'text-slate-400' : 'text-slate-600'}`}>{details.join(' · ')}</p>
        {p.sms_campaign && (
          <p className="text-xs text-slate-500">{t('promo.fromSms', { name: p.sms_campaign })}</p>
        )}
        {p.note && <p className="text-xs text-slate-500 break-words">{p.note}</p>}

        {(canEdit || p.version > 1) && (
          <Wrapper dir="row" gap="xs" align="center" wrap>
            {canEdit && !cancelled && (
              <Button size="sm" variant="secondary" onClick={() => onEdit(p)}>{t('promo.edit')}</Button>
            )}
            {canEdit && !cancelled && (
              <Button size="sm" variant="ghost" disabled={busy} onClick={() => onCancel(p)}>
                {t('promo.cancel')}
              </Button>
            )}
            {canEdit && cancelled && (
              <Button size="sm" variant="secondary" disabled={busy} onClick={() => onRestore(p)}>
                {t('promo.restore')}
              </Button>
            )}
            {p.version > 1 && (
              <Button
                size="sm"
                variant="ghost"
                aria-expanded={historyOpen}
                onClick={() => setHistoryOpen((open) => !open)}
              >
                {historyOpen ? t('promo.hideHistory') : t('promo.historyWithCount', { count: p.version })}
              </Button>
            )}
          </Wrapper>
        )}

        {historyOpen && (
          <PromotionHistory
            versions={history.data?.versions}
            isLoading={history.isLoading}
            error={history.error}
            onRetry={() => void history.refetch()}
          />
        )}
      </Wrapper>
    </li>
  )
})
