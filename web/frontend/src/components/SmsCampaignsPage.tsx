import { memo, useCallback, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { FlaskConical } from 'lucide-react'
import { PageShell } from './PageShell'
import { InfoBanner } from './InfoBanner'
import { SmsCampaignWizard, SmsWizardIntro } from './SmsCampaignWizard'
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
  // The chosen campaign lives here because both blocks below speak about it:
  // the list picks one, the results read it. They used to hold that choice
  // separately, so the list could not answer the only question a sent campaign
  // still has.
  const [selected, setSelected] = useState<string | null>(null)
  const resultsRef = useRef<HTMLElement>(null)

  const showResults = useCallback((campaign: string) => {
    setSelected(campaign)
    // Picking from the list is a request to read the results, and on a phone
    // they are a screen away.
    resultsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }, [])

  return (
    <PageShell variant="feature" ariaLabel={t('sms.title')}>
      <InfoBanner icon={FlaskConical} title={t('sms.howItWorks')}>
        <ol className="list-decimal list-outside ml-4 space-y-0.5">
          <li>{t('sms.how1')}</li>
          <li>{t('sms.how2')}</li>
          <li>{t('sms.how3')}</li>
        </ol>
        <p className="mt-1.5">{t('sms.howControl')}</p>
      </InfoBanner>

      {/* Step 1 keeps its place on the page whether or not a campaign is
          being built — collapsed, it is the panel header with the "new
          campaign" action; open, it is the wizard. Without this the page
          used to open on a bare "Step 2". */}
      <section aria-label={t('sms.wizardTitle')}>
        {building ? (
          <SmsCampaignWizard onClose={() => setBuilding(false)} />
        ) : (
          <SmsWizardIntro onStart={() => setBuilding(true)} />
        )}
      </section>

      <section aria-label={t('sms.campaignsTitle')}>
        <SmsCampaignList selected={selected} onSelect={showResults} />
      </section>
      <section aria-label={t('sms.resultsTitle')} ref={resultsRef}>
        <SmsCampaignResults campaign={selected} onCampaignChange={setSelected} />
      </section>
    </PageShell>
  )
})

export default SmsCampaignsPage
