import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { PageShell } from './PageShell'
import { PageHeading } from './PageHeading'
import { ChartSection } from './ChartSection'
import { ROICalculator } from './ROICalculator'
import { LazyPromocodeAnalyticsChart } from './chartsLazy'
import { MonthlyReport } from './MonthlyReport'
import { PromotionCalendar } from './PromotionCalendar'

export const MarketingPage = memo(function MarketingPage() {
  const { t } = useTranslation()

  return (
    <PageShell variant="feature">
      <PageHeading title={t('nav.marketing')} />
      <section>
        <MonthlyReport />
      </section>
      {/* Its own <section id="promotions">: /marketing#promotions lands there. */}
      <PromotionCalendar />
      <ChartSection>
        <LazyPromocodeAnalyticsChart />
      </ChartSection>
      <section>
        <ROICalculator />
      </section>
    </PageShell>
  )
})

export default MarketingPage
