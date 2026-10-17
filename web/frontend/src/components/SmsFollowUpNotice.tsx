import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { Button } from './Button'
import { formatDate, formatNumber } from '../utils/formatters'
import type { SmsFollowUpRef } from '../types/api'

// ─── SmsFollowUpNotice ───────────────────────────────────────────────────────
//
// What a follow-up is, said wherever one is handled.
//
// A follow-up is the same people messaged twice on purpose — a "last day"
// reminder. Built as an ordinary campaign it would draw a control of its own,
// and since the holdout is drawn per campaign name, about a tenth of the first
// campaign's control would be messaged by it: the first campaign's measurement
// gone, quietly. So a follow-up has no control, and its effect is read on the
// campaign it follows, as two messages against none.
//
// That has to be visible at the three moments it matters: when choosing whom
// it reaches, when pressing send, and when reading results that will show no
// comparison at all.

type Context =
  /** Building it, or about to send it: who gets the second message. */
  | 'send'
  /** Its own results, which compare nothing. */
  | 'results'

interface SmsFollowUpNoticeProps {
  /** The campaign this one follows. */
  root: string
  context: Context
  /** How many will get the second message, where known. */
  recipients?: number
  /** Opens the root's results — where this follow-up's effect is read. */
  onOpenRoot?: () => void
}

export const SmsFollowUpNotice = memo(function SmsFollowUpNotice({
  root, context, recipients, onOpenRoot,
}: SmsFollowUpNoticeProps) {
  const { t } = useTranslation()

  return (
    <div className="rounded-md border border-sky-200 bg-sky-50 px-3 py-2 text-xs
                    leading-snug text-sky-900">
      {context === 'send' && recipients != null && (
        <p className="font-medium tabular-nums">
          {t('sms.followUpRecipients', { n: formatNumber(recipients), root })}
        </p>
      )}
      <p className={context === 'send' && recipients != null ? 'mt-1' : undefined}>
        {t(context === 'send' ? 'sms.followUpNoControl' : 'sms.followUpResultsNote', {
          root,
        })}
      </p>
      {onOpenRoot && (
        <div className="mt-2">
          <Button size="sm" variant="secondary" onClick={onOpenRoot}>
            {t('sms.followUpOpenRoot', { root })}
          </Button>
        </div>
      )}
    </div>
  )
})

// ─── SmsFollowUpsLine ────────────────────────────────────────────────────────
//
// On a campaign that was followed up: by what, when, and to how many. Its
// lift then includes the second message those people got, and a reader who
// does not know that will credit the first message with both.

export const SmsFollowUpsLine = memo(function SmsFollowUpsLine({
  followUps,
}: {
  followUps: SmsFollowUpRef[]
}) {
  const { t } = useTranslation()
  if (followUps.length === 0) return null

  return (
    <div className="rounded-md border border-sky-200 bg-sky-50 px-3 py-2 text-xs
                    leading-snug text-sky-900">
      <p className="font-medium">{t('sms.followUpsTitle')}</p>
      <ul className="mt-1 space-y-0.5 tabular-nums">
        {followUps.map((f) => (
          <li key={f.campaign}>
            {t('sms.followUpsItem', {
              campaign: f.campaign,
              date: f.sentAt ? formatDate(f.sentAt) : t('sms.notSent'),
              recipients: formatNumber(f.recipients),
            })}
          </li>
        ))}
      </ul>
      <p className="mt-1">{t('sms.followUpsNote')}</p>
    </div>
  )
})
