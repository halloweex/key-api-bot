import { memo } from 'react'
import { TrafficSummaryCards } from './TrafficSummaryCards'
import { ROASSection } from './ROASSection'
import { PlatformBreakdownChart } from './PlatformBreakdownChart'
import { TrafficTrendChart } from './TrafficTrendChart'
import { TrafficTransactionsTable } from './TrafficTransactionsTable'

// ─── Component ────────────────────────────────────────────────────────────────

export const TrafficPage = memo(function TrafficPage() {
  return (
    <main className="flex-1 py-3 px-1 sm:py-4 sm:px-1.5 lg:py-6 lg:px-2 overflow-auto">
      <div className="max-w-[1800px] mx-auto space-y-4 sm:space-y-6">
        {/* Summary Cards */}
        <section aria-label="Traffic summary">
          <TrafficSummaryCards />
        </section>

        {/* Trend Chart - Full Width */}
        <section aria-label="Traffic trend">
          <TrafficTrendChart />
        </section>

        {/* Platform Breakdown */}
        <section aria-label="Platform breakdown">
          <PlatformBreakdownChart />
        </section>

        {/* Order Details Table */}
        <section aria-label="Traffic transactions">
          <TrafficTransactionsTable />
        </section>

        {/* ROAS Calculator */}
        <section aria-label="ROAS calculator">
          <ROASSection />
        </section>
      </div>
    </main>
  )
})

export default TrafficPage
