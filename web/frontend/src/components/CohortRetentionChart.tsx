import { memo, useState, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from './ChartContainer'
import {
  useCohortRetention,
  usePurchaseTiming,
  useCohortLTV,
  useAtRiskCustomers,
} from '../hooks'
import { RetentionMatrix, RetentionLegend } from './RetentionMatrix'
import { PurchaseTimingChart } from './PurchaseTimingChart'
import { CohortLTVChart } from './CohortLTVChart'
import { AtRiskTable } from './AtRiskTable'
import { RetentionInsights } from './RetentionInsights'
import { TabBar, TabButton } from './TabBar'
import { InfoBanner } from './InfoBanner'
import { CompactSelect } from './CompactSelect'
import { CohortTabSkeleton } from './CohortTabSkeleton'
import { CohortSummaryCards } from './CohortSummaryCards'
import {
  TABS,
  PERIOD_OPTIONS,
  DEPTH_OPTIONS,
  THRESHOLD_OPTIONS,
  type TabId,
} from './cohortTabsConfig'

export const CohortRetentionChart = memo(function CohortRetentionChart() {
  const { t } = useTranslation()
  const [activeTab, setActiveTab] = useState<TabId>('retention')
  const [monthsBack, setMonthsBack] = useState(12)
  const [retentionMonths, setRetentionMonths] = useState(6)
  const [daysThreshold, setDaysThreshold] = useState(90)

  const isRetentionTab = activeTab === 'retention' || activeTab === 'revenue'
  const cohortQuery = useCohortRetention(monthsBack, retentionMonths, true, isRetentionTab)
  const timingQuery = usePurchaseTiming(monthsBack, activeTab === 'timing')
  const ltvQuery = useCohortLTV(monthsBack, retentionMonths, activeTab === 'ltv')
  const atRiskQuery = useAtRiskCustomers(daysThreshold, monthsBack, activeTab === 'at-risk')

  const activeQuery = useMemo(() => {
    switch (activeTab) {
      case 'retention':
      case 'revenue':
        return cohortQuery
      case 'timing':
        return timingQuery
      case 'ltv':
        return ltvQuery
      case 'at-risk':
        return atRiskQuery
      default:
        return cohortQuery
    }
  }, [activeTab, cohortQuery, timingQuery, ltvQuery, atRiskQuery])

  const isEmpty = !activeQuery.isLoading && !activeQuery.data

  // M1 retention trend: latest complete cohort M1 vs weighted avg M1
  const m1Trend = useMemo(() => {
    if (!cohortQuery.data?.cohorts || !cohortQuery.data?.summary) return null
    const avgM1 = cohortQuery.data.summary.avgCustomerRetention?.[1]
    if (avgM1 == null) return null
    const latestWithM1 = cohortQuery.data.cohorts.find((c) => c.retention?.[1] != null)
    if (!latestWithM1 || latestWithM1.retention[1] == null) return null
    return Math.round((latestWithM1.retention[1] - avgM1) * 10) / 10
  }, [cohortQuery.data])

  const showPeriod = activeTab !== 'at-risk'
  const showDepth = activeTab === 'retention' || activeTab === 'revenue'
  const showThreshold = activeTab === 'at-risk'

  const renderContent = () => {
    if (activeQuery.isLoading) {
      return <CohortTabSkeleton tab={activeTab} />
    }

    switch (activeTab) {
      case 'retention':
        if (!cohortQuery.data?.cohorts) return null
        return (
          <>
            {isRetentionTab && cohortQuery.data && (
              <CohortSummaryCards
                data={cohortQuery.data}
                activeTab={activeTab}
                monthsBack={monthsBack}
                m1Trend={m1Trend}
              />
            )}
            <RetentionMatrix
              cohorts={cohortQuery.data.cohorts}
              retentionMonths={retentionMonths}
              type="customer"
            />
            <RetentionLegend />
            {cohortQuery.data.insights && (
              <RetentionInsights insights={cohortQuery.data.insights} type="customer" />
            )}
          </>
        )

      case 'revenue':
        if (!cohortQuery.data?.cohorts) return null
        return (
          <>
            {isRetentionTab && cohortQuery.data && (
              <CohortSummaryCards
                data={cohortQuery.data}
                activeTab={activeTab}
                monthsBack={monthsBack}
                m1Trend={m1Trend}
              />
            )}
            <RetentionMatrix
              cohorts={cohortQuery.data.cohorts}
              retentionMonths={retentionMonths}
              type="revenue"
            />
            <RetentionLegend />
            {cohortQuery.data.insights && (
              <RetentionInsights insights={cohortQuery.data.insights} type="revenue" />
            )}
          </>
        )

      case 'timing':
        if (!timingQuery.data) return null
        return <PurchaseTimingChart data={timingQuery.data} />

      case 'ltv':
        if (!ltvQuery.data) return null
        return <CohortLTVChart data={ltvQuery.data} />

      case 'at-risk':
        if (!atRiskQuery.data) return null
        return <AtRiskTable data={atRiskQuery.data} />

      default:
        return null
    }
  }

  const getInfoText = () => {
    switch (activeTab) {
      case 'retention':
        return t('retention.retentionExplain')
      case 'revenue':
        return t('retention.revenueExplain')
      case 'timing':
        return t('retention.timingExplain')
      case 'ltv':
        return t('retention.ltvExplain')
      case 'at-risk':
        return t('retention.atRiskExplain')
      default:
        return ''
    }
  }

  return (
    <ChartContainer
      title={t('retention.title')}
      isLoading={false}
      error={activeQuery.error as Error | null}
      onRetry={activeQuery.refetch}
      isEmpty={isEmpty}
      height="auto"
      ariaLabel="Cohort analysis with retention, revenue, timing, LTV, and at-risk customer views"
    >
      <TabBar variant="bordered" ariaLabel="Cohort analysis views">
        {TABS.map((tab) => (
          <TabButton
            key={tab.id}
            variant="bordered"
            active={activeTab === tab.id}
            onClick={() => setActiveTab(tab.id)}
          >
            <span className="hidden sm:inline">{t(tab.label)}</span>
            <span className="sm:hidden">{t(tab.shortLabel)}</span>
          </TabButton>
        ))}
      </TabBar>

      <div className="flex flex-wrap items-center gap-4 mb-4 px-1">
        {showPeriod && (
          <CompactSelect
            label={t('retention.period')}
            value={monthsBack}
            options={PERIOD_OPTIONS}
            onChange={setMonthsBack}
            suffix={t('retention.monthsUnit')}
          />
        )}
        {showDepth && (
          <CompactSelect
            label={t('retention.depth')}
            value={retentionMonths}
            options={DEPTH_OPTIONS}
            onChange={setRetentionMonths}
          />
        )}
        {showThreshold && (
          <>
            <CompactSelect
              label={t('retention.period')}
              value={monthsBack}
              options={PERIOD_OPTIONS}
              onChange={setMonthsBack}
              suffix={t('retention.monthsUnit')}
            />
            <CompactSelect
              label={t('retention.inactiveFor')}
              value={daysThreshold}
              options={THRESHOLD_OPTIONS}
              onChange={setDaysThreshold}
              suffix={t('retention.days')}
            />
          </>
        )}
      </div>

      <InfoBanner>
        <strong>{t('retention.howToRead')}</strong> {getInfoText()}
      </InfoBanner>

      {renderContent()}
    </ChartContainer>
  )
})
