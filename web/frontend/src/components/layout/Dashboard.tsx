import { Suspense, memo } from 'react'
import { SummaryCards } from '../cards'
import { SkeletonChart, ROICalculator } from '../ui'
import {
  LazyRevenueTrendChart,
  LazyOrdersBySourceChart,
  LazyRevenueBySourceChart,
  LazyTopProductsChart,
  LazyTopProductsByRevenueChart,
  LazyCategoryChart,
  LazyCustomerInsightsChart,
  LazyBrandAnalyticsChart,
  LazyExpensesChart,
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
    <section className="grid grid-cols-1 lg:grid-cols-2 gap-6">
      {children}
    </section>
  )
})

// ─── Dashboard Component ─────────────────────────────────────────────────────

export const Dashboard = memo(function Dashboard() {
  return (
    <main className="p-3 sm:p-6 lg:p-8 space-y-4 sm:space-y-6 max-w-[1800px] mx-auto">
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

      {/* Brand Analytics - Full Width */}
      <ChartSection>
        <LazyBrandAnalyticsChart />
      </ChartSection>

      {/* ROI Calculator */}
      <section>
        <ROICalculator />
      </section>

      {/* Expenses & Profit - Full Width */}
      <ChartSection>
        <LazyExpensesChart />
      </ChartSection>
    </main>
  )
})
