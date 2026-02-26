import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { TrafficSummaryCards } from './TrafficSummaryCards'
import { ROASSection } from './ROASSection'
import { PlatformBreakdownChart } from './PlatformBreakdownChart'
import { TrafficTrendChart } from './TrafficTrendChart'
import { TrafficTransactionsTable } from './TrafficTransactionsTable'

// ─── Component ────────────────────────────────────────────────────────────────

export const TrafficPage = memo(function TrafficPage() {
  const { t } = useTranslation()

  return (
    <main className="flex-1 py-3 px-1 sm:py-4 sm:px-1.5 lg:py-6 lg:px-2 overflow-auto">
      <div className="max-w-[1800px] mx-auto space-y-4 sm:space-y-6">
        {/* Summary Cards */}
        <section aria-label={t('traffic.summary')}>
          <TrafficSummaryCards />
        </section>

        {/* Trend Chart - Full Width */}
        <section aria-label={t('traffic.trend')}>
          <TrafficTrendChart />
        </section>

        {/* Platform Breakdown */}
        <section aria-label={t('traffic.platformBreakdown')}>
          <PlatformBreakdownChart />
        </section>

        {/* Order Details Table */}
        <section aria-label={t('traffic.transactions')}>
          <TrafficTransactionsTable />
        </section>

        {/* ROAS Calculator */}
        <section aria-label={t('traffic.roasCalculator')}>
          <ROASSection />
        </section>
      </div>
    </main>
  )
})

export default TrafficPage
