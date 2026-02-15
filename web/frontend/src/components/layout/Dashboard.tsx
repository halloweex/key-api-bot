import { Suspense, memo } from 'react'
import { SummaryCards } from '../cards'
import { SkeletonChart, ROICalculator, ProtectedSection } from '../ui'
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
  LazyStockSummaryChart,
  LazyDeadStockChart,
  LazyInventoryTrendChart,
  LazyManualExpensesTable,
} from '../charts/lazy'

// ─── Chart Loading Fallback ──────────────────────────────────────────────────

const ChartFallback = memo(function ChartFallback() {
  return <SkeletonChart />
})

// ─── Chart Section with Suspense ─────────────────────────────────────────────

interface ChartSectionProps {
  children: React.ReactNode
  className?: string
}

const ChartSection = memo(function ChartSection({
  children,
  className = '',
}: ChartSectionProps) {
  return (
    <section className={className}>
      <Suspense fallback={<ChartFallback />}>
        {children}
      </Suspense>
    </section>
  )
})

// ─── Grid Section ────────────────────────────────────────────────────────────

interface GridSectionProps {
  children: React.ReactNode
}

const GridSection = memo(function GridSection({ children }: GridSectionProps) {
  return (
    <section className="grid grid-cols-1 lg:grid-cols-2 gap-3 sm:gap-4">
      {children}
    </section>
  )
})

// ─── Dashboard Component ─────────────────────────────────────────────────────

export const Dashboard = memo(function Dashboard() {
  return (
    <main className="py-3 pr-0 pl-0 sm:py-4 sm:pr-0 sm:pl-0 lg:py-6 lg:pr-0 lg:pl-0 space-y-3 sm:space-y-4 max-w-[1800px]">
      {/* Summary Cards - loaded immediately */}
      <section>
        <SummaryCards />
      </section>

      {/* Revenue Trend - Full Width */}
      <ChartSection>
        <LazyRevenueTrendChart />
      </ChartSection>

      {/* Orders & Revenue by Source - Side by Side */}
      <GridSection>
        <ChartSection>
          <LazyOrdersBySourceChart />
        </ChartSection>
        <ChartSection>
          <LazyRevenueBySourceChart />
        </ChartSection>
      </GridSection>

      {/* Charts Row 2 - Top Products (Quantity & Revenue) */}
      <GridSection>
        <ChartSection>
          <LazyTopProductsChart />
        </ChartSection>
        <ChartSection>
          <LazyTopProductsByRevenueChart />
        </ChartSection>
      </GridSection>

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

      {/* Stock Summary & Dead Stock Analysis */}
      <GridSection>
        <ChartSection>
          <LazyStockSummaryChart />
        </ChartSection>
        <ChartSection>
          <LazyDeadStockChart />
        </ChartSection>
      </GridSection>

      {/* Inventory Trend - Full Width */}
      <ChartSection>
        <LazyInventoryTrendChart />
      </ChartSection>

      {/* ROI Calculator */}
      <section>
        <ROICalculator />
      </section>

      {/* Manual Expenses Table - Admin Only */}
      <ProtectedSection feature="expenses">
        <ChartSection>
          <LazyManualExpensesTable />
        </ChartSection>
      </ProtectedSection>
    </main>
  )
})
