import { Suspense, memo } from 'react'
import { SummaryCards } from '../cards'
import { SkeletonChart, ProtectedSection } from '../ui'
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
    <section className="grid grid-cols-1 lg:grid-cols-2 gap-1.5 sm:gap-2">
      {children}
    </section>
  )
})

// ─── Dashboard Component ─────────────────────────────────────────────────────

export const Dashboard = memo(function Dashboard() {
  return (
    <main className="py-1.5 px-1 sm:py-2 sm:px-1.5 lg:py-3 lg:px-2 space-y-1.5 sm:space-y-2 max-w-[1800px]">
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

      {/* Manual Expenses Table - Admin Only */}
      <ProtectedSection feature="expenses">
        <ChartSection>
          <LazyManualExpensesTable />
        </ChartSection>
      </ProtectedSection>
    </main>
  )
})
