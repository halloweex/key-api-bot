import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { FlaskConical, Plus } from 'lucide-react'
import { PageShell } from './PageShell'
import { InfoBanner } from './InfoBanner'
import { Button } from './Button'
import { SmsCampaignWizard } from './SmsCampaignWizard'
import { SmsCampaignList } from './SmsCampaignList'
import { SmsCampaignResults } from './SmsCampaignResults'

// ─── SmsCampaignsPage ────────────────────────────────────────────────────────
//
// The page follows the campaign's own order: build one, watch the ones that
// exist, read what they did.
//
// It used to lead with a fixed cohort — three value tiers over a 270-day
// window — and a CSV download. That cohort was the only audience the page
// could express, and taking the file was the only way to create a campaign at
// all, so "who is this for" had exactly one answer and nobody could see it
// written down. Now the audience is built in the wizard, from filters, and the
// old cohort is one of the presets it offers.

export const SmsCampaignsPage = memo(function SmsCampaignsPage() {
  const { t } = useTranslation()
  const [building, setBuilding] = useState(false)

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

      {building ? (
        <section aria-label={t('sms.wizardTitle')}>
          <SmsCampaignWizard onClose={() => setBuilding(false)} />
        </section>
      ) : (
        <div className="flex justify-end">
          <Button onClick={() => setBuilding(true)}>
            <Plus className="w-4 h-4" /> {t('sms.newCampaign')}
          </Button>
        </div>
      )}

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
