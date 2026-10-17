import { memo } from 'react'
import { useTranslation } from 'react-i18next'

// ─── SmsAudienceSource ───────────────────────────────────────────────────────
//
// Where a campaign's people come from: a new audience, or the people a sent
// campaign already reached.
//
// The second is its own choice, at the top of the first step, rather than a
// filter among the others — because it is not a narrowing of anything. A
// follow-up has no window, no tiers and no control of its own, and the wizard
// skips the steps that would ask for them.

export const SmsAudienceSource = memo(function SmsAudienceSource({
  followUp,
  onChange,
}: {
  followUp: boolean
  onChange: (followUp: boolean) => void
}) {
  const { t } = useTranslation()
  const options = [
    { value: false, label: t('sms.sourceAudience') },
    { value: true, label: t('sms.sourceFollowUp') },
  ]

  return (
    <div role="radiogroup" aria-label={t('sms.sourceLabel')} className="flex flex-wrap gap-2">
      {options.map((o) => (
        <button
          key={String(o.value)}
          type="button"
          role="radio"
          aria-checked={followUp === o.value}
          onClick={() => onChange(o.value)}
          className={`px-3 py-1.5 text-xs rounded-md border transition-colors ${
            followUp === o.value
              ? 'border-purple-400 bg-purple-50 text-purple-800 font-medium'
              : 'border-slate-200 text-slate-600 hover:border-slate-300'
          }`}
        >
          {o.label}
        </button>
      ))}
    </div>
  )
})
