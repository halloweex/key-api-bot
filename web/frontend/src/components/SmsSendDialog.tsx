import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Button } from './Button'
import { SmsCostLine } from './SmsCostLine'
import { SmsChannelFields, composeSmsText } from './SmsChannelFields'
import { SmsFollowUpNotice } from './SmsFollowUpNotice'
import { SmsSendBudget } from './SmsSendBudget'
import { SmsSendBreakdown } from './SmsSendBreakdown'
import {
  useSendRestOfSmsCampaign, useSendSmsCampaign, useSmsBalance, useSmsSendProgress,
} from '../hooks/useApi'
import { useToast } from './Toast'
import { formatNumber } from '../utils/formatters'
import { balanceIsShort, smsCost, type SmsBalanceReading } from '../utils/smsCost'
import { describeSmsError, refusalReason } from '../utils/smsRefusal'
import { useLayer } from '../utils/layerStack'
import type { SmsCampaignSummary, SmsChannel, SmsSendResult } from '../types/api'

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
//
// The name typed back travels with the send and the server checks it too, so
// it is not a browser-only gate. Beside it: recipients, cost and the gateway's
// balance, read afresh when the dialog opens — a send the balance does not
// cover is refused by the server, so the button says so first.
//
// A send can break off (2026-11-26: a batch past the gateway's timeout). The
// dialog then says, from the roster, how many got the message, how many the
// gateway gave no answer for and how many it never reached, and offers to send
// the rest: only those never reached, with the campaign's own text. `mode`
// "rest" opens it for that alone, from the campaign list after a reload.

const SMS_LIMIT = 600

interface SmsSendDialogProps {
  campaign: SmsCampaignSummary
  /** The text the campaign was written, priced and rehearsed with. */
  initialText?: string
  /** "rest": send the campaign's message to the targets it never reached. */
  mode?: 'send' | 'rest'
  onClose: () => void
}

export const SmsSendDialog = memo(function SmsSendDialog({
  campaign,
  initialText = '',
  mode = 'send',
  onClose,
}: SmsSendDialogProps) {
  const { t } = useTranslation()
  const restOnly = mode === 'rest'
  // Carried in from the step that wrote it. Opening empty here made the
  // manager retype a text that had already been counted, priced and rehearsed
  // — and what went out could then differ from what was tested.
  const [text, setText] = useState(initialText)
  const [channel, setChannel] = useState<SmsChannel>('sms')
  const [buttonCaption, setButtonCaption] = useState('')
  const [buttonUrl, setButtonUrl] = useState('')
  const [confirmName, setConfirmName] = useState('')
  // One press per dialog, whatever the outcome. A send that appears to fail
  // may still be running at the gateway, and pressing again is how a roster
  // gets messaged twice — the server refuses the second attempt now, but the
  // control that produced the mistake belongs here too. The same for the rest.
  const [attempted, setAttempted] = useState(false)
  const [restAttempted, setRestAttempted] = useState(false)
  // What went wrong, said in the dialog itself and not only in a toast. A
  // toast is gone in seconds, and on 2026-11-26 the dialog sat on "Sending…"
  // with nothing on screen to say the send had broken halfway.
  const [failure, setFailure] = useState<string | null>(null)
  const send = useSendSmsCampaign()
  const rest = useSendRestOfSmsCampaign()
  const busy = send.isPending || rest.isPending
  // The send answers only at the end, about 4.5 s per 1 000 recipients, and
  // records each batch as it lands: the roster is the progress bar. Read once
  // more after a send, for the outcome; at once in "rest" mode, for the count.
  const progress = useSmsSendProgress(campaign.campaign, {
    enabled: restOnly || busy || attempted || restAttempted,
    poll: busy,
  })
  const { addToast } = useToast()
  const { data: gateway, isLoading: balanceLoading } = useSmsBalance()
  // On the stack of what "back" closes first — Telegram's BackButton — except
  // mid-send, where the dialog's own Cancel is disabled too.
  useLayer(true, () => { if (!busy) onClose() })

  const trimmed = text.trim()
  const hybrid = channel === 'viber_sms'
  // The SMS arm has no button, so it carries the link inline — and it is the
  // arm the cost is charged on.
  const smsText = useMemo(
    () => (hybrid ? composeSmsText(trimmed, buttonUrl) : trimmed),
    [hybrid, trimmed, buttonUrl],
  )
  const cost = useMemo(
    () => smsCost(restOnly ? campaign.messageText ?? '' : smsText),
    [restOnly, campaign.messageText, smsText],
  )
  const buttonComplete = Boolean(buttonCaption.trim()) === Boolean(buttonUrl.trim())
  const nameMatches = confirmName.trim() === campaign.campaign
  const balance: SmsBalanceReading = balanceLoading
    ? { state: 'loading' }
    : gateway?.balance != null
      ? { state: 'known', amount: gateway.balance }
      : { state: 'unknown' }
  const price = gateway?.pricePerPart ?? null
  const notSent = progress.data?.notSent ?? 0
  const offerRest = !restOnly && attempted && !busy && notSent > 0
  // What the next press would send, and so what the balance has to cover.
  const recipients = restOnly || offerRest ? notSent : campaign.target
  // Only a balance known to be short holds the button. An unknown one is left
  // to the server, which reads it again and refuses if it still cannot —
  // a blip when the dialog opened must not block a send that is covered.
  const short = balanceIsShort(balance, recipients, cost.parts, price)
  const canSend = trimmed.length > 0 && smsText.length <= SMS_LIMIT && nameMatches
    && (!hybrid || buttonComplete) && !short && !busy && !attempted
  // The rest: once the send is over, to whoever it never reached, once.
  const canSendRest = notSent > 0 && nameMatches && !busy && !restAttempted && !short

  function reportFailure(error: Error) {
    // A refusal the server named — the balance, today's other campaign,
    // the name typed back — is said in the reader's language and is not
    // "the gateway refused": it never reached the gateway.
    const reason = refusalReason(error)
    const named = reason != null
    const message = describeSmsError(error, t)
    // "Not sent" is false for a send that broke off with some of it
    // already on people's phones.
    const title = reason === 'gateway_outcome_unknown'
      ? t('sms.sendBroke')
      : named ? t('sms.sendRefused') : t('sms.sendFailed')
    setFailure(message ?? title)
    addToast({ type: 'error', title, message, duration: named ? 20000 : undefined })
  }

  function reportSent(result: SmsSendResult, title: string) {
    setFailure(null)
    addToast({
      type: result.unsent > 0 ? 'error' : 'success',
      title,
      message: result.partialError ?? undefined,
      duration: result.unsent > 0 ? 20000 : 8000,
    })
    // A roster sent whole is done. A partial one stays open on its outcome,
    // with the rest one press away.
    if (result.unsent === 0) onClose()
  }

  function handleSend() {
    setAttempted(true)
    send.mutate(
      {
        campaign: campaign.campaign,
        text: smsText,
        confirmCampaign: confirmName.trim(),
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
        onSuccess: (result) => reportSent(result, result.unsent > 0
          ? t('sms.sendPartial', { unsent: result.unsent })
          : t('sms.sendDone', {
              accepted: result.accepted,
              stoplisted: result.stoplisted,
              failed: result.failed,
            })),
        onError: reportFailure,
      },
    )
  }

  function handleSendRest() {
    setRestAttempted(true)
    rest.mutate(
      { campaign: campaign.campaign, confirmCampaign: confirmName.trim() },
      {
        onSuccess: (result) => reportSent(result, result.unsent > 0
          ? t('sms.sendPartial', { unsent: result.unsent })
          : t('sms.restDone', { accepted: formatNumber(result.accepted) })),
        onError: reportFailure,
      },
    )
  }

  const title = restOnly
    ? t('sms.restTitle', { campaign: campaign.campaign })
    : t('sms.sendTitle', { campaign: campaign.campaign })

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      role="dialog"
      aria-modal="true"
      aria-label={title}
    >
      <div className="bg-white rounded-xl shadow-xl w-full max-w-lg max-h-full overflow-auto">
        <div className="px-5 py-4 border-b border-slate-100">
          <h2 className="text-base font-semibold text-slate-800">{title}</h2>
          <p className="text-xs text-slate-500 mt-1">
            {restOnly
              ? t('sms.restDesc', { n: formatNumber(notSent) })
              : t('sms.sendRecipients', {
                  target: formatNumber(campaign.target),
                  holdout: formatNumber(campaign.holdout),
                })}
          </p>
        </div>

        <div className="px-5 py-4 space-y-4">
          {restOnly ? (
            <div>
              <p className="text-xs font-medium text-slate-700">{t('sms.messageText')}</p>
              <p className="mt-1 rounded-lg bg-slate-50 px-3 py-2 text-sm text-slate-700
                            whitespace-pre-wrap break-words">
                {campaign.messageText}
              </p>
            </div>
          ) : (
            <>
              <label className="block">
                <span className="text-xs font-medium text-slate-700">
                  {t('sms.messageText')}
                </span>
                <textarea
                  value={text}
                  onChange={(e) => setText(e.target.value)}
                  rows={4}
                  autoFocus
                  disabled={attempted}
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

              {campaign.followUpOf && (
                <SmsFollowUpNotice
                  root={campaign.followUpOf} context="send" recipients={campaign.target}
                />
              )}
            </>
          )}

          {(restOnly || busy || attempted || restAttempted) && (
            <div className="space-y-1.5">
              <SmsSendBreakdown
                progress={progress.data}
                total={campaign.target}
                running={busy}
              />
              {busy && (
                <p className="text-[11px] text-slate-500 leading-snug">
                  {t('sms.sendProgressHint')}
                </p>
              )}
            </div>
          )}

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

          <SmsSendBudget
            recipients={recipients}
            parts={cost.parts}
            pricePerPart={price}
            balance={balance}
          />

          {failure && !busy && (
            <p role="alert"
               className="text-xs text-rose-800 bg-rose-50 rounded-md px-3 py-2 leading-snug">
              {failure}
            </p>
          )}
        </div>

        <div className="px-5 py-4 border-t border-slate-100 flex justify-end gap-2">
          <Button variant="secondary" size="sm" onClick={onClose} disabled={busy}>
            {(attempted || restAttempted) && !busy ? t('sms.close') : t('sms.cancel')}
          </Button>
          {restOnly || offerRest ? (
            <Button size="sm" onClick={handleSendRest} disabled={!canSendRest}>
              {rest.isPending
                ? t('sms.sending')
                : t('sms.sendRest', { n: formatNumber(notSent) })}
            </Button>
          ) : (
            <Button size="sm" onClick={handleSend} disabled={!canSend}>
              {send.isPending
                ? t('sms.sending')
                : t('sms.sendNow', { total: formatNumber(campaign.target) })}
            </Button>
          )}
        </div>
      </div>
    </div>
  )
})
