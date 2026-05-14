import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { PageShell } from './PageShell'
import { TabBar, TabButton } from './TabBar'
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
    <PageShell variant="feature">
      <TabBar variant="filled" ariaLabel="Report sections">
        {TAB_KEYS.map((tab) => (
          <TabButton
            key={tab.key}
            variant="filled"
            active={activeTab === tab.key}
            onClick={() => setActiveTab(tab.key)}
          >
            {t(tab.labelKey)}
          </TabButton>
        ))}
      </TabBar>

      {activeTab === 'summary' && <SummaryTab />}
      {activeTab === 'top_products' && <TopProductsTab />}
    </PageShell>
  )
})

export default ReportsPage
