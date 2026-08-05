import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { useSmsChannels } from '../hooks/useApi'
import type { SmsChannel } from '../types/api'

// ─── SmsChannelFields ────────────────────────────────────────────────────────
//
// Choosing the channel, and the button that only one of them can show.
//
// Viber and SMS are not two qualities of the same message. Viber carries a
// labelled button — the only way a campaign link ever reads as words rather
// than as a bare URL — and the SMS fallback has none, so the link has to be
// spelled out in its text instead. That asymmetry is the whole reason the two
// texts are composed separately, and it is stated here rather than discovered
// when the fallback arrives looking broken.
//
// The Viber option is disabled unless a Viber sender is registered: those are
// approved separately from SMS alpha names, so a working SMS setup says
// nothing about whether Viber will be accepted.

/** What the SMS arm shows: the message, with the link on its own line. */
export function composeSmsText(base: string, buttonUrl: string): string {
  const text = base.trim()
  const url = buttonUrl.trim()
  if (!url || text.includes(url)) return text
  return text ? `${text}\n${url}` : url
}

const CAPTION_LIMIT = 30

export const SmsChannelFields = memo(function SmsChannelFields({
  channel,
  onChannelChange,
  buttonCaption,
  onButtonCaptionChange,
  buttonUrl,
  onButtonUrlChange,
}: {
  channel: SmsChannel
  onChannelChange: (c: SmsChannel) => void
  buttonCaption: string
  onButtonCaptionChange: (v: string) => void
  buttonUrl: string
  onButtonUrlChange: (v: string) => void
}) {
  const { t } = useTranslation()
  const { data: channels } = useSmsChannels()
  const viberAvailable = channels?.viber ?? false

  const options: { value: SmsChannel; label: string; disabled: boolean }[] = [
    { value: 'sms', label: t('sms.channelSms'), disabled: false },
    { value: 'viber_sms', label: t('sms.channelViberSms'), disabled: !viberAvailable },
  ]

  return (
    <div className="space-y-3">
      <div>
        <span className="text-xs font-medium text-slate-700">{t('sms.channel')}</span>
        <div className="mt-1 flex gap-2" role="radiogroup" aria-label={t('sms.channel')}>
          {options.map((o) => (
            <button
              key={o.value}
              type="button"
              role="radio"
              aria-checked={channel === o.value}
              disabled={o.disabled}
              onClick={() => onChannelChange(o.value)}
              className={`px-3 py-1.5 text-xs rounded-md border transition-colors ${
                channel === o.value
                  ? 'border-purple-400 bg-purple-50 text-purple-800 font-medium'
                  : 'border-slate-200 text-slate-600 hover:border-slate-300'
              } disabled:opacity-50 disabled:cursor-not-allowed`}
            >
              {o.label}
            </button>
          ))}
        </div>
        {!viberAvailable && (
          <p className="mt-1 text-[11px] text-slate-500">{t('sms.viberUnavailable')}</p>
        )}
      </div>

      {channel === 'viber_sms' && (
        <div className="space-y-2 rounded-md border border-slate-200 p-3">
          <p className="text-[11px] text-slate-500 leading-snug">
            {t('sms.viberButtonHint')}
          </p>
          <div className="flex flex-wrap gap-2">
            <label className="text-xs text-slate-600 flex-1 min-w-[10rem]">
              <span className="block mb-1">{t('sms.buttonCaption')}</span>
              <input
                type="text"
                value={buttonCaption}
                onChange={(e) => onButtonCaptionChange(e.target.value)}
                maxLength={CAPTION_LIMIT}
                placeholder="Korean Story"
                className="w-full px-2 py-1.5 text-sm bg-white border border-slate-200
                           rounded-md text-slate-700 focus:outline-none focus:ring-2
                           focus:ring-purple-500/30 focus:border-purple-400"
              />
            </label>
            <label className="text-xs text-slate-600 flex-1 min-w-[12rem]">
              <span className="block mb-1">{t('sms.buttonUrl')}</span>
              <input
                type="url"
                value={buttonUrl}
                onChange={(e) => onButtonUrlChange(e.target.value)}
                placeholder="https://"
                className="w-full px-2 py-1.5 text-sm bg-white border border-slate-200
                           rounded-md text-slate-700 focus:outline-none focus:ring-2
                           focus:ring-purple-500/30 focus:border-purple-400"
              />
            </label>
          </div>
          {/* A caption with no destination is refused by the gateway, so say
              it here rather than letting the send come back 400. */}
          {Boolean(buttonCaption.trim()) !== Boolean(buttonUrl.trim()) && (
            <p className="text-[11px] text-amber-700">{t('sms.buttonNeedsBoth')}</p>
          )}
        </div>
      )}
    </div>
  )
})
