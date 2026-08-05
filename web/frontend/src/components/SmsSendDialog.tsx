import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Button } from './Button'
import { SmsCostLine } from './SmsCostLine'
import { SmsChannelFields, composeSmsText } from './SmsChannelFields'
import { useSendSmsCampaign } from '../hooks/useApi'
import { useToast } from './Toast'
import { formatNumber } from '../utils/formatters'
import { smsCost } from '../utils/smsCost'
import type { SmsCampaignSummary, SmsChannel } from '../types/api'

// ─── SmsSendDialog ───────────────────────────────────────────────────────────
//
// Sending is irreversible in the way that matters: the roster is stamped sent,
// a second send is refused, and the messages are already at people's phones.
// So the control asks for the text, states plainly how many will receive it,
// and requires the campaign name typed back before it will fire.
//
// The count shown is the target arm only. The control group is named
// explicitly beside it, because "why did 618 people not get it" is the
// question this screen has to answer before it is asked.

const SMS_LIMIT = 600

interface SmsSendDialogProps {
  campaign: SmsCampaignSummary
  onClose: () => void
}

export const SmsSendDialog = memo(function SmsSendDialog({
  campaign,
  onClose,
}: SmsSendDialogProps) {
  const { t } = useTranslation()
  const [text, setText] = useState('')
  const [channel, setChannel] = useState<SmsChannel>('sms')
  const [buttonCaption, setButtonCaption] = useState('')
  const [buttonUrl, setButtonUrl] = useState('')
  const [confirmName, setConfirmName] = useState('')
  const send = useSendSmsCampaign()
  const { addToast } = useToast()

  const trimmed = text.trim()
  const hybrid = channel === 'viber_sms'
  // The SMS arm has no button, so it carries the link inline — and it is the
  // arm the cost is charged on.
  const smsText = useMemo(
    () => (hybrid ? composeSmsText(trimmed, buttonUrl) : trimmed),
    [hybrid, trimmed, buttonUrl],
  )
  const cost = useMemo(() => smsCost(smsText), [smsText])
  const buttonComplete = Boolean(buttonCaption.trim()) === Boolean(buttonUrl.trim())
  const nameMatches = confirmName.trim() === campaign.campaign
  const canSend = trimmed.length > 0 && smsText.length <= SMS_LIMIT && nameMatches
    && (!hybrid || buttonComplete) && !send.isPending

  function handleSend() {
    send.mutate(
      {
        campaign: campaign.campaign,
        text: smsText,
        channel,
        viber: hybrid
          ? {
              viberText: trimmed,
              buttonCaption: buttonCaption.trim() || undefined,
              buttonUrl: buttonUrl.trim() || undefined,
            }
          : undefined,
      },
      {
        onSuccess: (result) => {
          // A partly-sent roster is not a success: the campaign is stamped, so
          // the rest can never be messaged without sending to the others twice.
          addToast({
            type: result.unsent > 0 ? 'error' : 'success',
            title: result.unsent > 0
              ? t('sms.sendPartial', { unsent: result.unsent })
              : t('sms.sendDone', {
                  accepted: result.accepted,
                  stoplisted: result.stoplisted,
                  failed: result.failed,
                }),
            message: result.partialError ?? undefined,
            duration: result.unsent > 0 ? 20000 : 8000,
          })
          onClose()
        },
        onError: (error) => {
          addToast({
            type: 'error',
            title: t('sms.sendFailed'),
            message: error instanceof Error ? error.message : undefined,
          })
        },
      },
    )
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      role="dialog"
      aria-modal="true"
      aria-label={t('sms.sendTitle', { campaign: campaign.campaign })}
    >
      <div className="bg-white rounded-xl shadow-xl w-full max-w-lg max-h-full overflow-auto">
        <div className="px-5 py-4 border-b border-slate-100">
          <h2 className="text-base font-semibold text-slate-800">
            {t('sms.sendTitle', { campaign: campaign.campaign })}
          </h2>
          <p className="text-xs text-slate-500 mt-1">
            {t('sms.sendRecipients', {
              target: formatNumber(campaign.target),
              holdout: formatNumber(campaign.holdout),
            })}
          </p>
        </div>

        <div className="px-5 py-4 space-y-4">
          <label className="block">
            <span className="text-xs font-medium text-slate-700">
              {t('sms.messageText')}
            </span>
            <textarea
              value={text}
              onChange={(e) => setText(e.target.value)}
              rows={4}
              autoFocus
              className="mt-1 w-full px-3 py-2 text-sm border border-slate-200 rounded-lg
                         text-slate-700 focus:outline-none focus:ring-2
                         focus:ring-purple-500/30 focus:border-purple-400 resize-y"
              placeholder={t('sms.messagePlaceholder')}
            />
            <SmsCostLine cost={cost} limit={SMS_LIMIT} recipients={campaign.target} />
          </label>

          <SmsChannelFields
            channel={channel}
            onChannelChange={setChannel}
            buttonCaption={buttonCaption}
            onButtonCaptionChange={setButtonCaption}
            buttonUrl={buttonUrl}
            onButtonUrlChange={setButtonUrl}
          />

          {hybrid && smsText !== trimmed && (
            <div className="text-xs text-slate-600 bg-slate-50 rounded-md px-3 py-2">
              <p className="font-medium text-slate-700">{t('sms.smsFallbackPreview')}</p>
              <p className="mt-1 whitespace-pre-wrap break-words">{smsText}</p>
            </div>
          )}

          {campaign.promocode && (
            <p className="text-xs text-slate-600 bg-slate-50 rounded-md px-3 py-2">
              {t('sms.promoReminder', { code: campaign.promocode })}
            </p>
          )}

          <p className="text-xs text-amber-800 bg-amber-50 rounded-md px-3 py-2 leading-snug">
            {t('sms.sendIrreversible')}
          </p>

          <label className="block">
            <span className="text-xs font-medium text-slate-700">
              {t('sms.typeNameToConfirm', { campaign: campaign.campaign })}
            </span>
            <input
              type="text"
              value={confirmName}
              onChange={(e) => setConfirmName(e.target.value)}
              className="mt-1 w-full px-3 py-2 text-sm border border-slate-200 rounded-lg
                         text-slate-700 focus:outline-none focus:ring-2
                         focus:ring-purple-500/30 focus:border-purple-400"
            />
          </label>
        </div>

        <div className="px-5 py-4 border-t border-slate-100 flex justify-end gap-2">
          <Button variant="secondary" size="sm" onClick={onClose}
                  disabled={send.isPending}>
            {t('sms.cancel')}
          </Button>
          <Button size="sm" onClick={handleSend} disabled={!canSend}>
            {send.isPending
              ? t('sms.sending')
              : t('sms.sendNow', { total: formatNumber(campaign.target) })}
          </Button>
        </div>
      </div>
    </div>
  )
})
