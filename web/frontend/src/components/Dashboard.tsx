import { memo } from 'react'
import { SummaryCards } from './SummaryCards'
import { PageShell } from './PageShell'
import { ChartSection } from './ChartSection'
import { ChartGrid } from './ChartGrid'
import { ProtectedSection } from './ProtectedSection'
import {
  LazyRevenueTrendChart,
  LazyOrdersBySourceChart,
  LazyRevenueBySourceChart,
  LazyTopProductsChart,
  LazyTopProductsByRevenueChart,
  LazyCategoryChart,
  LazyCustomerInsightsChart,
  LazyCohortRetentionChart,
  LazyBrandAnalyticsChart,
  LazyManualExpensesTable,
} from './chartsLazy'

// ─── Dashboard ───────────────────────────────────────────────────────────────
//
// The main sales dashboard: summary cards, then the chart panels in reading
// order. Pure composition — every panel owns its own data and visuals.

export const Dashboard = memo(function Dashboard() {
  return (
    <PageShell variant="dashboard">
      {/* Summary Cards - loaded immediately */}
      <section>
        <SummaryCards />
      </section>

      {/* Revenue Trend - Full Width */}
      <ChartSection>
        <LazyRevenueTrendChart />
      </ChartSection>

      {/* Orders & Revenue by Source - Side by Side */}
      <ChartGrid density="dense">
        <ChartSection>
          <LazyOrdersBySourceChart />
        </ChartSection>
        <ChartSection>
          <LazyRevenueBySourceChart />
        </ChartSection>
      </ChartGrid>

      {/* Charts Row 2 - Top Products (Quantity & Revenue) */}
      <ChartGrid density="dense">
        <ChartSection>
          <LazyTopProductsChart />
        </ChartSection>
        <ChartSection>
          <LazyTopProductsByRevenueChart />
        </ChartSection>
      </ChartGrid>

      {/* Charts Row 3 - Category Breakdown */}
      <ChartSection>
        <LazyCategoryChart />
      </ChartSection>

      {/* Customer Insights - Full Width */}
      <ChartSection>
        <LazyCustomerInsightsChart />
      </ChartSection>

      {/* Cohort Retention Analysis - Full Width */}
      <ChartSection>
        <LazyCohortRetentionChart />
      </ChartSection>

      {/* Brand Analytics - Full Width */}
      <ChartSection>
        <LazyBrandAnalyticsChart />
      </ChartSection>

      {/* Manual Expenses Table - Admin Only */}
      <ProtectedSection feature="expenses">
        <ChartSection>
          <LazyManualExpensesTable />
        </ChartSection>
      </ProtectedSection>
    </PageShell>
  )
})
