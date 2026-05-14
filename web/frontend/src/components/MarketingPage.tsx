import { memo, Suspense } from 'react'
import { useTranslation } from 'react-i18next'
import { PageShell } from './PageShell'
import { ROICalculator } from './ROICalculator'
import { SkeletonChart } from './Skeleton'
import { LazyPromocodeAnalyticsChart } from './chartsLazy'
import { MonthlyReport } from './MonthlyReport'

export const MarketingPage = memo(function MarketingPage() {
  const { t } = useTranslation()

  return (
    <PageShell variant="feature">
      <h1 className="text-xl sm:text-2xl font-bold text-slate-800">
        {t('nav.marketing')}
      </h1>
      <section>
        <MonthlyReport />
      </section>
      <section>
        <Suspense fallback={<SkeletonChart />}>
          <LazyPromocodeAnalyticsChart />
        </Suspense>
      </section>
      <section>
        <ROICalculator />
      </section>
    </PageShell>
  )
})

export default MarketingPage
