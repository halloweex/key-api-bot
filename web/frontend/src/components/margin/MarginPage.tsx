import { memo } from 'react'
import { MarginSummaryCards } from './MarginSummaryCards'
import { MarginTrendChart } from './MarginTrendChart'
import { MarginByBrandChart } from './MarginByBrandChart'
import { MarginByCategoryChart } from './MarginByCategoryChart'
import { MarginAlerts } from './MarginAlerts'
import { MarginBrandCategoryTable } from './MarginBrandCategoryTable'

// ─── Component ───────────────────────────────────────────────────────────────

const MarginPage = memo(function MarginPage() {
  return (
    <main className="py-3 px-1 sm:py-4 sm:px-3 md:px-4 lg:px-6">
      <div className="max-w-[1800px] mx-auto space-y-4 sm:space-y-5">
        {/* KPI Summary Cards */}
        <MarginSummaryCards />

        {/* Margin Trend (monthly bars + margin% line) */}
        <section aria-label="Margin trend">
          <MarginTrendChart />
        </section>

        {/* Brand & Category side by side */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-5">
          <section aria-label="Margin by brand">
            <MarginByBrandChart />
          </section>
          <section aria-label="Margin by category">
            <MarginByCategoryChart />
          </section>
        </div>

        {/* Low-margin alerts */}
        <section aria-label="Low-margin alerts">
          <MarginAlerts />
        </section>

        {/* Brand x Category cross-tab */}
        <section aria-label="Brand category cross-tab">
          <MarginBrandCategoryTable />
        </section>
      </div>
    </main>
  )
})

export default MarginPage
