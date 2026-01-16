import { Suspense, memo } from 'react'
import { SummaryCards } from '../cards'
import { SkeletonChart } from '../ui'
import {
  LazyRevenueTrendChart,
  LazySalesBySourceChart,
  LazyTopProductsChart,
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
    <main className="p-6 space-y-6">
      {/* Summary Cards - loaded immediately */}
      <section>
        <SummaryCards />
      </section>

      {/* Charts Row 1 - Revenue & Source */}
      <GridSection>
        <ChartSection>
          <LazyRevenueTrendChart />
        </ChartSection>
        <ChartSection>
          <LazySalesBySourceChart />
        </ChartSection>
      </GridSection>

      {/* Charts Row 2 - Products & Category */}
      <GridSection>
        <ChartSection>
          <LazyTopProductsChart />
        </ChartSection>
        <ChartSection>
          <LazyCategoryChart />
        </ChartSection>
      </GridSection>

      {/* Customer Insights - Full Width */}
      <ChartSection>
        <LazyCustomerInsightsChart />
      </ChartSection>

      {/* Brand Analytics - Full Width */}
      <ChartSection>
        <LazyBrandAnalyticsChart />
      </ChartSection>

      {/* Expenses & Profit - Full Width */}
      <ChartSection>
        <LazyExpensesChart />
      </ChartSection>
    </main>
  )
})
