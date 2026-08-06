import { memo, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { InfoPopover } from './InfoPopover'

// ─── SmsResultsGuide ─────────────────────────────────────────────────────────
//
// What every figure on the results panel actually is.
//
// All of them are differences against a control group, and that is the one
// thing the labels cannot convey alone. Read as earnings, "added margin" is
// alarming when it turns negative; read as the result, the messaged group's
// own purchase rate says a campaign worked when it did nothing. Both readings
// happened on the first real campaign, so the explanation belongs on the page.
//
// It hangs off the same "?" control the other tabs use, rather than inventing
// a disclosure of its own.

function Entry({ term, children }: { term: string; children: ReactNode }) {
  return (
    <div className="py-1.5 border-t border-slate-700 first:border-t-0 first:pt-0">
      <dt className="text-slate-200 font-medium">{term}</dt>
      <dd className="text-slate-400 leading-snug mt-0.5">{children}</dd>
    </div>
  )
}

export const SmsResultsGuide = memo(function SmsResultsGuide() {
  const { t } = useTranslation()

  return (
    <InfoPopover title={t('sms.guideTitle')} size="wide">
      <dl className="text-xs">
        <Entry term={t('sms.guideControlTerm')}>{t('sms.guideControlBody')}</Entry>
        <Entry term={t('sms.guideDiffTerm')}>{t('sms.guideDiffBody')}</Entry>
        <Entry term={t('sms.lift')}>{t('sms.guideLiftBody')}</Entry>
        <Entry term={t('sms.pValue')}>{t('sms.guidePValueBody')}</Entry>
        <Entry term={t('sms.guideVerdictTerm')}>{t('sms.guideVerdictBody')}</Entry>
        <Entry term={t('sms.guideRevenueTerm')}>{t('sms.guideRevenueBody')}</Entry>
        <Entry term={t('sms.guideMarginTerm')}>{t('sms.guideMarginBody')}</Entry>
        <Entry term={t('sms.guideWindowTerm')}>{t('sms.guideWindowBody')}</Entry>
        <Entry term={t('sms.guideChangingTerm')}>{t('sms.guideChangingBody')}</Entry>
      </dl>
    </InfoPopover>
  )
})
