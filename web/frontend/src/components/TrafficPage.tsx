import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { PageShell } from './PageShell'
import { TrafficSectionNav } from './TrafficSectionNav'
import { TrafficSummaryCards } from './TrafficSummaryCards'
import { ROASSection } from './ROASSection'
import { PlatformBreakdownChart } from './PlatformBreakdownChart'
import { OrganicBreakdown } from './OrganicBreakdown'
import { PixelOnlyBreakdown } from './PixelOnlyBreakdown'
import { SiteFunnel } from './SiteFunnel'
import { TrafficTrendChart } from './TrafficTrendChart'
import { TrafficTransactionsTable } from './TrafficTransactionsTable'
import { UtmCampaignsTable } from './UtmCampaignsTable'
import { useRouter } from '../hooks/useRouter'
import { trafficTabFor } from '../utils/trafficTabs'

// ─── Component ────────────────────────────────────────────────────────────────
//
// Three sections, one question each (`utils/trafficTabs`): where the site
// loses buyers, which channels bring orders, which campaigns and whether the
// ads pay. Each is its own path, so only its blocks are asked for.

export const TrafficPage = memo(function TrafficPage() {
  const { t } = useTranslation()
  const tab = trafficTabFor(useRouter()) ?? 'funnel'

  return (
    <PageShell variant="feature">
      <TrafficSectionNav tab={tab} />

      {tab === 'funnel' && (
        <section aria-label={t('traffic.funnelTitle')}>
          <SiteFunnel />
        </section>
      )}

      {tab === 'sales' && (
        <>
          <section aria-label={t('traffic.summary')}>
            <TrafficSummaryCards />
          </section>
          <section aria-label={t('traffic.trend')}>
            <TrafficTrendChart />
          </section>
          <section aria-label={t('traffic.platformBreakdown')}>
            <PlatformBreakdownChart />
          </section>
          <section aria-label={t('traffic.organicBreakdown')}>
            <OrganicBreakdown />
          </section>
          <section aria-label={t('traffic.pixelOnlyBreakdown')}>
            <PixelOnlyBreakdown />
          </section>
        </>
      )}

      {tab === 'campaigns' && (
        <>
          <section aria-label={t('traffic.utmCampaigns')}>
            <UtmCampaignsTable />
          </section>
          <section aria-label={t('traffic.transactions')}>
            <TrafficTransactionsTable />
          </section>
          <section aria-label={t('traffic.roasCalculator')}>
            <ROASSection />
          </section>
        </>
      )}
    </PageShell>
  )
})

export default TrafficPage
