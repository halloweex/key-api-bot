import { memo } from 'react'
import { PageShell } from './PageShell'
import { ChartGrid } from './ChartGrid'
import { MarginSummaryCards } from './MarginSummaryCards'
import { MarginTrendChart } from './MarginTrendChart'
import { MarginByBrandChart } from './MarginByBrandChart'
import { MarginByCategoryChart } from './MarginByCategoryChart'
import { MarginAlerts } from './MarginAlerts'
import { MarginBrandCategoryTable } from './MarginBrandCategoryTable'

// ─── Component ───────────────────────────────────────────────────────────────

export const MarginPage = memo(function MarginPage() {
  return (
    <PageShell variant="feature">
      {/* KPI Summary Cards */}
      <MarginSummaryCards />

      {/* Margin Trend (monthly bars + margin% line) */}
      <section aria-label="Margin trend">
        <MarginTrendChart />
      </section>

      {/* Brand & Category side by side */}
      <ChartGrid>
        <section aria-label="Margin by brand">
          <MarginByBrandChart />
        </section>
        <section aria-label="Margin by category">
          <MarginByCategoryChart />
        </section>
      </ChartGrid>

      {/* Low-margin alerts */}
      <section aria-label="Low-margin alerts">
        <MarginAlerts />
      </section>

      {/* Brand x Category cross-tab */}
      <section aria-label="Brand category cross-tab">
        <MarginBrandCategoryTable />
      </section>
    </PageShell>
  )
})

export default MarginPage
