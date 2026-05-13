import { Suspense, memo, useState } from 'react'
import { SkeletonChart } from './Skeleton'
import {
  LazyStockSummaryChart,
  LazyDeadStockChart,
  LazyInventoryTurnoverChart,
  LazyInventoryTrendChart,
  LazyBrandRotationCard,
  LazySkuRotationTable,
} from './chartsLazy'

const ChartFallback = () => <SkeletonChart />

type SkuPreset = 'all' | 'discount' | 'reorder' | 'skip' | 'decelerating'

export const InventoryPage = memo(function InventoryPage() {
  const [skuBrandFilter, setSkuBrandFilter] = useState<string | null>(null)
  const [skuPreset, setSkuPreset] = useState<SkuPreset | null>(null)

  // Click on brand row in BrandRotationCard → drill into Discount preset filtered to that brand
  const onBrandClick = (brand: string) => {
    setSkuBrandFilter(brand)
    setSkuPreset('discount')
  }

  const onClearExternalFilter = () => {
    setSkuBrandFilter(null)
    setSkuPreset(null)
  }

  return (
    <main className="flex-1 py-3 px-1 sm:py-4 sm:px-1.5 lg:py-6 lg:px-2 overflow-auto">
      <div className="max-w-[1800px] mx-auto space-y-4 sm:space-y-6">
        <section className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
          <Suspense fallback={<ChartFallback />}>
            <LazyStockSummaryChart />
          </Suspense>
          <Suspense fallback={<ChartFallback />}>
            <LazyDeadStockChart />
          </Suspense>
        </section>

        <section>
          <Suspense fallback={<ChartFallback />}>
            <LazyInventoryTurnoverChart />
          </Suspense>
        </section>

        <section>
          <Suspense fallback={<ChartFallback />}>
            <LazyBrandRotationCard onBrandClick={onBrandClick} />
          </Suspense>
        </section>

        <section>
          <Suspense fallback={<ChartFallback />}>
            <LazySkuRotationTable
              brandFilter={skuBrandFilter}
              presetOverride={skuPreset}
              onClearExternalFilter={onClearExternalFilter}
            />
          </Suspense>
        </section>

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
