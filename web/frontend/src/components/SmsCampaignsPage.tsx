import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { FlaskConical } from 'lucide-react'
import { PageShell } from './PageShell'
import { InfoBanner } from './InfoBanner'
import { SmsSegmentCards } from './SmsSegmentCards'
import { SmsCampaignList } from './SmsCampaignList'
import { SmsCampaignResults } from './SmsCampaignResults'

// ─── SmsCampaignsPage ────────────────────────────────────────────────────────
//
// The page follows the campaign's own order: choose the tiers, take the file,
// record that it went out, then read what it did.
//
// That order was legible to whoever built it and to nobody else — three cards
// stacked with no sign they were a sequence. So the cards are numbered and the
// banner states the loop up front, including the one rule that makes the last
// step mean anything: a slice of every tier is deliberately never messaged.

export const SmsCampaignsPage = memo(function SmsCampaignsPage() {
  const { t } = useTranslation()

  return (
    <PageShell variant="feature" ariaLabel={t('sms.title')}>
      <InfoBanner icon={<FlaskConical className="w-4 h-4" />} title={t('sms.howItWorks')}>
        <ol className="list-decimal list-outside ml-4 space-y-0.5">
          <li>{t('sms.how1')}</li>
          <li>{t('sms.how2')}</li>
          <li>{t('sms.how3')}</li>
        </ol>
        <p className="mt-1.5">{t('sms.howControl')}</p>
      </InfoBanner>

      <section aria-label={t('sms.segmentsTitle')}>
        <SmsSegmentCards />
      </section>
      <section aria-label={t('sms.campaignsTitle')}>
        <SmsCampaignList />
      </section>
      <section aria-label={t('sms.resultsTitle')}>
        <SmsCampaignResults />
      </section>
    </PageShell>
  )
})

export default SmsCampaignsPage
