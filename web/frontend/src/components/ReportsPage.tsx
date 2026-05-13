import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { SummaryTab } from './SummaryTab'
import { TopProductsTab } from './TopProductsTab'

type Tab = 'summary' | 'top_products'

const TAB_KEYS: { key: Tab; labelKey: string }[] = [
  { key: 'summary', labelKey: 'reports.summary' },
  { key: 'top_products', labelKey: 'reports.topProducts' },
]

export const ReportsPage = memo(function ReportsPage() {
  const { t } = useTranslation()
  const [activeTab, setActiveTab] = useState<Tab>('summary')

  return (
    <main className="flex-1 py-3 px-1 sm:py-4 sm:px-1.5 lg:py-6 lg:px-2 overflow-auto">
      <div className="max-w-[1800px] mx-auto space-y-4 sm:space-y-6">
        <div className="flex gap-1 bg-slate-100 p-1 rounded-xl w-fit">
          {TAB_KEYS.map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                activeTab === tab.key
                  ? 'bg-white text-slate-900 shadow-sm'
                  : 'text-slate-500 hover:text-slate-700'
              }`}
            >
              {t(tab.labelKey)}
            </button>
          ))}
        </div>

        {activeTab === 'summary' && <SummaryTab />}
        {activeTab === 'top_products' && <TopProductsTab />}
      </div>
    </main>
  )
})

export default ReportsPage
