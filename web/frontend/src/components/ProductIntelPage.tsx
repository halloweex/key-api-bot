import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { PageShell } from './PageShell'
import { BasketSummaryCards } from './BasketSummaryCards'
import { FrequentlyBoughtTogether } from './FrequentlyBoughtTogether'
import { BasketDistributionChart } from './BasketDistributionChart'
import { CategoryCombosChart } from './CategoryCombosChart'
import { BrandAffinityChart } from './BrandAffinityChart'
import { ProductMomentumTable } from './ProductMomentumTable'

export const ProductIntelPage = memo(function ProductIntelPage() {
  const { t } = useTranslation()
  return (
    <PageShell variant="feature">
      <section aria-label={t('products.basketSummary')}>
        <BasketSummaryCards />
      </section>
      <section aria-label={t('products.frequentlyBoughtTogether')}>
        <FrequentlyBoughtTogether />
      </section>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
        <section aria-label={t('products.basketDistribution')}>
          <BasketDistributionChart />
        </section>
        <section aria-label={t('products.categoryCombinations')}>
          <CategoryCombosChart />
        </section>
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
        <section aria-label={t('products.brandAffinity')}>
          <BrandAffinityChart />
        </section>
        <section aria-label={t('products.productMomentum')}>
          <ProductMomentumTable />
        </section>
      </div>
    </PageShell>
  )
})

export default ProductIntelPage
