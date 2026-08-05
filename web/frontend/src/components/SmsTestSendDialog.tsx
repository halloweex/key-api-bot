import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Button } from './Button'
import { SmsCostLine } from './SmsCostLine'
import { useSendTestSms } from '../hooks/useApi'
import { useToast } from './Toast'
import { smsCost } from '../utils/smsCost'
import type { SmsTestSendResult } from '../types/api'

// ─── SmsTestSendDialog ───────────────────────────────────────────────────────
//
// One message, one number, before any of it is real.
//
// Until this existed the only way to see a text as a customer sees it was to
// send an actual campaign — which stamps the roster sent and cannot be taken
// back. So a rehearsal writes nothing: no campaign, no roster, no opt-out.
//
// The gateway's own answer is shown rather than a success tick, because the
// interesting outcomes are the quiet ones: a stoplisted number reports
// "accepted: false" and needs to be read, not celebrated.

const SMS_LIMIT = 600
const PHONE_HINT = /^380\d{9}$/

export const SmsTestSendDialog = memo(function SmsTestSendDialog({
  initialText = '',
  onClose,
}: {
  initialText?: string
  onClose: () => void
}) {
  const { t } = useTranslation()
  const [phone, setPhone] = useState('')
  const [text, setText] = useState(initialText)
  const [result, setResult] = useState<SmsTestSendResult | null>(null)
  const send = useSendTestSms()
  const { addToast } = useToast()

  const trimmed = text.trim()
  const digits = useMemo(() => phone.replace(/\D/g, ''), [phone])
  const phoneValid = PHONE_HINT.test(digits)
  const cost = useMemo(() => smsCost(trimmed), [trimmed])

  const canSend = phoneValid && trimmed.length > 0
    && trimmed.length <= SMS_LIMIT && !send.isPending

  function handleSend() {
    setResult(null)
    send.mutate(
      { phone: digits, text: trimmed },
      {
        onSuccess: (r) => {
          setResult(r)
          if (!r.accepted) {
            addToast({
              type: 'error',
              title: r.stoplisted ? t('sms.testStoplisted') : t('sms.testRefused'),
              message: r.status || undefined,
              duration: 8000,
            })
          }
        },
        onError: (error) => {
          addToast({
            type: 'error',
            title: t('sms.testFailed'),
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
      aria-label={t('sms.testTitle')}
    >
      <div className="bg-white rounded-xl shadow-xl w-full max-w-lg max-h-full overflow-auto">
        <div className="px-5 py-4 border-b border-slate-100">
          <h2 className="text-base font-semibold text-slate-800">{t('sms.testTitle')}</h2>
          <p className="text-xs text-slate-500 mt-1">{t('sms.testDesc')}</p>
        </div>

        <div className="px-5 py-4 space-y-4">
          <label className="block">
            <span className="text-xs font-medium text-slate-700">{t('sms.testPhone')}</span>
            <input
              type="tel"
              value={phone}
              onChange={(e) => setPhone(e.target.value)}
              autoFocus
              placeholder="+380 93 455 55 54"
              className="mt-1 w-full px-3 py-2 text-sm border border-slate-200 rounded-lg
                         text-slate-700 focus:outline-none focus:ring-2
                         focus:ring-purple-500/30 focus:border-purple-400"
            />
            {digits.length > 0 && !phoneValid && (
              <span className="text-[11px] text-amber-700">{t('sms.testPhoneInvalid')}</span>
            )}
          </label>

          <label className="block">
            <span className="text-xs font-medium text-slate-700">{t('sms.messageText')}</span>
            <textarea
              value={text}
              onChange={(e) => setText(e.target.value)}
              rows={5}
              className="mt-1 w-full px-3 py-2 text-sm border border-slate-200 rounded-lg
                         text-slate-700 focus:outline-none focus:ring-2
                         focus:ring-purple-500/30 focus:border-purple-400 resize-y"
              placeholder={t('sms.messagePlaceholder')}
            />
            <SmsCostLine cost={cost} limit={SMS_LIMIT} />
          </label>

          <p className="text-xs text-slate-600 bg-slate-50 rounded-md px-3 py-2 leading-snug">
            {t('sms.testNoLinks')}
          </p>

          {result && (
            <div
              className={`text-xs rounded-md px-3 py-2 leading-snug ${
                result.accepted
                  ? 'text-emerald-800 bg-emerald-50'
                  : 'text-amber-800 bg-amber-50'
              }`}
              role="status"
            >
              {result.accepted
                ? t('sms.testAccepted', { id: result.messageId ?? '—' })
                : t('sms.testNotAccepted', {
                    code: result.code,
                    status: result.status || '—',
                  })}
            </div>
          )}
        </div>

        <div className="px-5 py-4 border-t border-slate-100 flex justify-end gap-2">
          <Button variant="secondary" size="sm" onClick={onClose} disabled={send.isPending}>
            {result ? t('sms.close') : t('sms.cancel')}
          </Button>
          <Button size="sm" onClick={handleSend} disabled={!canSend}>
            {send.isPending ? t('sms.sending') : t('sms.testSendNow')}
          </Button>
        </div>
      </div>
    </div>
  )
})
