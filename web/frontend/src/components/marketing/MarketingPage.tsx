import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { ROICalculator } from '../ui/ROICalculator'

export default memo(function MarketingPage() {
  const { t } = useTranslation()

  return (
    <main className="p-4 sm:p-6 max-w-7xl mx-auto space-y-6">
      <h1 className="text-xl sm:text-2xl font-bold text-slate-800">
        {t('nav.marketing')}
      </h1>

      <section>
        <ROICalculator />
      </section>
    </main>
  )
})
