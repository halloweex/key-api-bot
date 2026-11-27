import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { Button } from './Button'
import { useSmsSendProgress } from '../hooks/useApi'
import { formatNumber } from '../utils/formatters'

// ─── SmsSendShortfall ────────────────────────────────────────────────────────
//
// Under a sent campaign in the list: what its send did not reach, and the way
// to send the rest. Says nothing for a campaign sent whole, which is nearly
// all of them. Lives in the list so a send that broke off can be finished after
// the page was closed. The dialog that showed the outcome is gone by then.

interface SmsSendShortfallProps {
  campaign: string
  /** Opens the send in "rest" mode. Without it, the counts are said alone. */
  onSendRest?: () => void
}

export const SmsSendShortfall = memo(function SmsSendShortfall({
  campaign, onSendRest,
}: SmsSendShortfallProps) {
  const { t } = useTranslation()
  const { data } = useSmsSendProgress(campaign)
  if (!data || (data.notSent === 0 && data.uncertain === 0)) return null

  const said = [
    data.notSent > 0 && t('sms.shortfall.notSent', { n: formatNumber(data.notSent) }),
    data.uncertain > 0 && t('sms.shortfall.uncertain', { n: formatNumber(data.uncertain) }),
  ].filter(Boolean).join(' · ')

  return (
    <div className="mt-1 space-y-1">
      <p className="text-[11px] text-amber-700 tabular-nums">{said}</p>
      {data.notSent > 0 && onSendRest && (
        <Button size="sm" variant="secondary" onClick={onSendRest}>
          {t('sms.sendRest', { n: formatNumber(data.notSent) })}
        </Button>
      )}
    </div>
  )
})
