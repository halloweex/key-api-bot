import { memo, Suspense } from 'react'
import { useTranslation } from 'react-i18next'
import { ROICalculator } from '../ui/ROICalculator'
import { SkeletonChart } from '../ui'
import { LazyPromocodeAnalyticsChart } from '../charts/lazy'
import { MonthlyReport } from './MonthlyReport'

export default memo(function MarketingPage() {
  const { t } = useTranslation()

  return (
    <main className="flex-1 py-3 px-1 sm:py-4 sm:px-1.5 lg:py-6 lg:px-2 overflow-auto">
      <div className="max-w-[1800px] mx-auto space-y-6">
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
      </div>
    </main>
  )
})
