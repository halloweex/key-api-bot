import { memo, useState } from 'react'
import { PageShell } from './PageShell'
import { ChartSection } from './ChartSection'
import { ChartGrid } from './ChartGrid'
import {
  LazyStockSummaryChart,
  LazyDeadStockChart,
  LazyInventoryTurnoverChart,
  LazyInventoryTrendChart,
  LazyBrandRotationCard,
  LazySkuRotationTable,
} from './chartsLazy'

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
    <PageShell variant="feature">
      <ChartGrid>
        <ChartSection>
          <LazyStockSummaryChart />
        </ChartSection>
        <ChartSection>
          <LazyDeadStockChart />
        </ChartSection>
      </ChartGrid>
      <ChartSection>
        <LazyInventoryTurnoverChart />
      </ChartSection>
      <ChartSection>
        <LazyBrandRotationCard onBrandClick={onBrandClick} />
      </ChartSection>
      <ChartSection>
        <LazySkuRotationTable
          brandFilter={skuBrandFilter}
          presetOverride={skuPreset}
          onClearExternalFilter={onClearExternalFilter}
        />
      </ChartSection>
      <ChartSection>
        <LazyInventoryTrendChart />
      </ChartSection>
    </PageShell>
  )
})

export default InventoryPage
