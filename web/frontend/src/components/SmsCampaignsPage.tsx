import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { PageShell } from './PageShell'
import { SmsSegmentCards } from './SmsSegmentCards'
import { SmsCampaignList } from './SmsCampaignList'
import { SmsCampaignResults } from './SmsCampaignResults'

// ─── SmsCampaignsPage ────────────────────────────────────────────────────────
//
// The page follows the campaign's own order: choose the tiers, take the file,
// record that it went out, then read what it did.

export const SmsCampaignsPage = memo(function SmsCampaignsPage() {
  const { t } = useTranslation()

  return (
    <PageShell variant="feature" ariaLabel={t('sms.title')}>
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
