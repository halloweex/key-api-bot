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

// ─── Coming Soon Overlay ──────────────────────────────────────────────────────

interface ComingSoonProps {
  children: React.ReactNode
  message?: string
}

const ComingSoon = memo(function ComingSoon({
  children,
  message = "In Development"
}: ComingSoonProps) {
  return (
    <div className="relative">
      <div className="blur-sm pointer-events-none select-none opacity-80">
        {children}
      </div>
      <div className="absolute inset-0 flex items-center justify-center">
        <div className="bg-slate-800/90 text-white px-6 py-3 rounded-xl shadow-lg flex items-center gap-3">
          <svg className="w-5 h-5 text-amber-400" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
          </svg>
          <span className="font-medium">{message}</span>
        </div>
      </div>
    </div>
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

      {/* Expenses & Profit - Full Width (Coming Soon) */}
      <ComingSoon message="Expenses — Coming soon">
        <ChartSection>
          <LazyExpensesChart />
        </ChartSection>
      </ComingSoon>
    </main>
  )
})
