import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { PageShell } from './PageShell'
import { TrafficSummaryCards } from './TrafficSummaryCards'
import { ROASSection } from './ROASSection'
import { PlatformBreakdownChart } from './PlatformBreakdownChart'
import { TrafficTrendChart } from './TrafficTrendChart'
import { TrafficTransactionsTable } from './TrafficTransactionsTable'

// ─── Component ────────────────────────────────────────────────────────────────

export const TrafficPage = memo(function TrafficPage() {
  const { t } = useTranslation()

  return (
    <PageShell variant="feature">
      <section aria-label={t('traffic.summary')}>
        <TrafficSummaryCards />
      </section>
      <section aria-label={t('traffic.trend')}>
        <TrafficTrendChart />
      </section>
      <section aria-label={t('traffic.platformBreakdown')}>
        <PlatformBreakdownChart />
      </section>
      <section aria-label={t('traffic.transactions')}>
        <TrafficTransactionsTable />
      </section>
      <section aria-label={t('traffic.roasCalculator')}>
        <ROASSection />
      </section>
    </PageShell>
  )
})

export default TrafficPage
