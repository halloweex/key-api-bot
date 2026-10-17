import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { Select } from './Select'
import { SmsFollowUpNotice } from './SmsFollowUpNotice'
import { formatDate, formatNumber } from '../utils/formatters'
import type { SmsCampaignSummary } from '../types/api'

// ─── SmsFollowUpPicker ───────────────────────────────────────────────────────
//
// The audience of a follow-up: one choice, the campaign it follows.
//
// Everything else an audience has — window, tiers, filters, a control — is
// deliberately absent. The server refuses any of them beside a follow-up,
// because a narrowed follow-up is a new campaign that has quietly lost its
// control group, and a follow-up with a control of its own would draw it from
// people the first campaign already messaged.

interface SmsFollowUpPickerProps {
  /** Sent campaigns of their own from the last week (`followUpCandidates`). */
  candidates: SmsCampaignSummary[]
  value: string | null
  onChange: (campaign: string | null) => void
  /** The previewed roster size for the chosen campaign, once known. */
  recipients?: number
}

export const SmsFollowUpPicker = memo(function SmsFollowUpPicker({
  candidates, value, onChange, recipients,
}: SmsFollowUpPickerProps) {
  const { t } = useTranslation()

  if (candidates.length === 0) {
    return (
      <p className="text-sm text-slate-600 bg-slate-50 border border-slate-200 rounded-lg
                    px-3 py-2">
        {t('sms.followUpNone')}
      </p>
    )
  }

  return (
    <div className="space-y-3">
      <label className="block text-xs text-slate-600">
        <span className="block mb-1">{t('sms.followUpPick')}</span>
        <Select
          options={candidates.map((c) => ({
            value: c.campaign,
            label: t('sms.followUpOption', {
              campaign: c.campaign,
              date: c.sentAt ? formatDate(c.sentAt) : '—',
              recipients: formatNumber(c.recipientsSent ?? c.target),
            }),
          }))}
          value={value}
          onChange={onChange}
          placeholder={t('sms.followUpPickPlaceholder')}
          variant="compact"
          aria-label={t('sms.followUpPick')}
        />
      </label>

      {value && (
        <SmsFollowUpNotice root={value} context="send" recipients={recipients} />
      )}
    </div>
  )
})
