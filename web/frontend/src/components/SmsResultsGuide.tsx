import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { ChevronDown, ChevronUp } from 'lucide-react'

// ─── SmsResultsGuide ─────────────────────────────────────────────────────────
//
// How to read the panel above.
//
// Every figure here is a *difference* against a control group, and that is the
// one thing the labels cannot convey on their own. A reader who takes "added
// margin" for earnings will find it alarming when it goes negative, and a
// reader who takes the messaged group's own rate for the result will believe
// a campaign worked when it did nothing. Both mistakes were made on the first
// real campaign, which is why this is on the page rather than in a doc.
//
// Collapsed by default: it is an explanation, not a control.

function Entry({ term, children }: { term: string; children: React.ReactNode }) {
  return (
    <div className="py-1.5 border-t border-slate-100">
      <dt className="text-slate-700 font-medium">{term}</dt>
      <dd className="text-slate-600 leading-snug mt-0.5">{children}</dd>
    </div>
  )
}

export const SmsResultsGuide = memo(function SmsResultsGuide() {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)

  return (
    <div className="mb-4">
      <button
        onClick={() => setOpen(!open)}
        aria-expanded={open}
        className="w-full flex items-center justify-between gap-3 text-left group"
      >
        <span className="text-xs font-medium text-slate-700">
          {t('sms.guideTitle')}
        </span>
        <span className="text-slate-400 group-hover:text-slate-600 transition-colors flex-shrink-0">
          {open ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
        </span>
      </button>

      {open && (
        <dl className="mt-2 text-xs">
          <Entry term={t('sms.guideDiffTerm')}>{t('sms.guideDiffBody')}</Entry>
          <Entry term={t('sms.lift')}>{t('sms.guideLiftBody')}</Entry>
          <Entry term={t('sms.pValue')}>{t('sms.guidePValueBody')}</Entry>
          <Entry term={t('sms.guideRevenueTerm')}>{t('sms.guideRevenueBody')}</Entry>
          <Entry term={t('sms.guideMarginTerm')}>{t('sms.guideMarginBody')}</Entry>
          <Entry term={t('sms.guideWindowTerm')}>{t('sms.guideWindowBody')}</Entry>
          <Entry term={t('sms.guideChangingTerm')}>{t('sms.guideChangingBody')}</Entry>
        </dl>
      )}
    </div>
  )
})
