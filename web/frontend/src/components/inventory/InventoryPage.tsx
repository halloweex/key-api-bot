import { Suspense, memo } from 'react'
import { SkeletonChart } from '../ui'
import {
  LazyStockSummaryChart,
  LazyDeadStockChart,
  LazyInventoryTrendChart,
} from '../charts/lazy'

const ChartFallback = () => <SkeletonChart />

export const InventoryPage = memo(function InventoryPage() {
  return (
    <main className="flex-1 p-3 sm:p-4 lg:p-6 overflow-auto">
      <div className="max-w-[1800px] mx-auto space-y-4 sm:space-y-6">
        {/* Stock Summary & Dead Stock Analysis */}
        <section className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
          <Suspense fallback={<ChartFallback />}>
            <LazyStockSummaryChart />
          </Suspense>
          <Suspense fallback={<ChartFallback />}>
            <LazyDeadStockChart />
          </Suspense>
        </section>

        {/* Inventory Trend - Full Width */}
        <section>
          <Suspense fallback={<ChartFallback />}>
            <LazyInventoryTrendChart />
          </Suspense>
        </section>
      </div>
    </main>
  )
})

export default InventoryPage
