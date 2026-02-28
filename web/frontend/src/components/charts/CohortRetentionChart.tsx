import { memo, useState, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { Users, TrendingUp, Repeat } from 'lucide-react'
import { ChartContainer } from './ChartContainer'
import {
  useCohortRetention,
  usePurchaseTiming,
  useCohortLTV,
  useAtRiskCustomers
} from '../../hooks'
import { formatNumber, formatPercent, formatCurrency } from '../../utils/formatters'
import {
  RetentionMatrix,
  RetentionLegend,
  PurchaseTimingChart,
  CohortLTVChart,
  AtRiskTable,
  SummaryCard,
  RetentionInsights
} from './retention'
import { SkeletonCard, SkeletonRetentionMatrix, SkeletonVerticalBars, SkeletonTable } from '../ui/Skeleton'

// ─── Tab Types ───────────────────────────────────────────────────────────────

type TabId = 'retention' | 'revenue' | 'timing' | 'ltv' | 'at-risk'

interface Tab {
  id: TabId
  label: string
  shortLabel: string
}

const TABS: Tab[] = [
  { id: 'retention', label: 'retention.customerRetention', shortLabel: 'retention.tabRetention' },
  { id: 'revenue', label: 'retention.revenueRetention', shortLabel: 'retention.tabRevenue' },
  { id: 'timing', label: 'retention.purchaseTiming', shortLabel: 'retention.tabTiming' },
  { id: 'ltv', label: 'retention.lifetimeValue', shortLabel: 'retention.tabLTV' },
  { id: 'at-risk', label: 'retention.atRiskCustomers', shortLabel: 'retention.tabAtRisk' },
]

// Period options
const PERIOD_OPTIONS = [
  { value: 6, label: '6' },
  { value: 12, label: '12' },
  { value: 18, label: '18' },
  { value: 24, label: '24' },
]

// Depth options (retention months)
const DEPTH_OPTIONS = [
  { value: 3, label: 'M0-M3' },
  { value: 6, label: 'M0-M6' },
  { value: 9, label: 'M0-M9' },
  { value: 12, label: 'M0-M12' },
]

// Days threshold options
const THRESHOLD_OPTIONS = [
  { value: 30, label: '30' },
  { value: 60, label: '60' },
  { value: 90, label: '90' },
  { value: 180, label: '180' },
  { value: 365, label: '365' },
]

// ─── Tab Button ──────────────────────────────────────────────────────────────

interface TabButtonProps {
  tab: Tab
  isActive: boolean
  onClick: () => void
}

const TabButton = memo(function TabButton({ tab, isActive, onClick }: TabButtonProps) {
  const { t } = useTranslation()
  return (
    <button
      onClick={onClick}
      className={`
        px-3 py-2 text-sm font-medium rounded-lg transition-colors whitespace-nowrap
        ${isActive
          ? 'bg-blue-100 text-blue-700 border border-blue-200'
          : 'text-slate-600 hover:bg-slate-100 border border-transparent'
        }
      `}
    >
      <span className="hidden sm:inline">{t(tab.label)}</span>
      <span className="sm:hidden">{t(tab.shortLabel)}</span>
    </button>
  )
})

// ─── Select ──────────────────────────────────────────────────────────────────

interface CompactSelectProps {
  label: string
  value: number
  options: { value: number; label: string }[]
  onChange: (v: number) => void
  suffix?: string
}

const CompactSelect = memo(function CompactSelect({ label, value, options, onChange, suffix }: CompactSelectProps) {
  return (
    <div className="flex items-center gap-1.5 text-sm">
      <span className="text-slate-500 font-medium">{label}:</span>
      <select
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="bg-white border border-slate-200 rounded-md px-2 py-1 text-sm text-slate-700 focus:ring-1 focus:ring-blue-400 focus:border-blue-400 outline-none"
      >
        {options.map(opt => (
          <option key={opt.value} value={opt.value}>
            {opt.label}{suffix ? ` ${suffix}` : ''}
          </option>
        ))}
      </select>
    </div>
  )
})

// ─── Skeleton per tab ────────────────────────────────────────────────────────

function TabSkeleton({ tab }: { tab: TabId }) {
  if (tab === 'retention' || tab === 'revenue') {
    return (
      <>
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-6">
          {[...Array(4)].map((_, i) => <SkeletonCard key={i} />)}
        </div>
        <SkeletonRetentionMatrix />
      </>
    )
  }
  if (tab === 'timing' || tab === 'ltv') {
    return (
      <>
        <div className="grid grid-cols-3 gap-4 mb-6">
          {[...Array(3)].map((_, i) => <SkeletonCard key={i} />)}
        </div>
        <SkeletonVerticalBars />
      </>
    )
  }
  // at-risk
  return (
    <>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {[...Array(4)].map((_, i) => <SkeletonCard key={i} />)}
      </div>
      <SkeletonTable />
    </>
  )
}

// ─── Component ───────────────────────────────────────────────────────────────

export const CohortRetentionChart = memo(function CohortRetentionChart() {
  const { t } = useTranslation()
  const [activeTab, setActiveTab] = useState<TabId>('retention')
  const [monthsBack, setMonthsBack] = useState(12)
  const [retentionMonths, setRetentionMonths] = useState(6)
  const [daysThreshold, setDaysThreshold] = useState(90)

  // Lazy: only fetch data for the active tab
  const isRetentionTab = activeTab === 'retention' || activeTab === 'revenue'
  const cohortQuery = useCohortRetention(monthsBack, retentionMonths, true, isRetentionTab)
  const timingQuery = usePurchaseTiming(monthsBack, activeTab === 'timing')
  const ltvQuery = useCohortLTV(monthsBack, retentionMonths, activeTab === 'ltv')
  const atRiskQuery = useAtRiskCustomers(daysThreshold, monthsBack, activeTab === 'at-risk')

  // Get the active query based on tab
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
    // Find latest cohort with M1 data
    const latestWithM1 = cohortQuery.data.cohorts.find(c => c.retention?.[1] != null)
    if (!latestWithM1 || latestWithM1.retention[1] == null) return null
    return Math.round((latestWithM1.retention[1] - avgM1) * 10) / 10
  }, [cohortQuery.data])

  // Control bar visibility
  const showPeriod = activeTab !== 'at-risk'
  const showDepth = activeTab === 'retention' || activeTab === 'revenue'
  const showThreshold = activeTab === 'at-risk'

  // Summary cards for retention/revenue tabs
  const renderSummaryCards = () => {
    if (!isRetentionTab) return null
    const data = cohortQuery.data
    if (!data?.summary) return null

    const avgRetention = activeTab === 'retention'
      ? data.summary.avgCustomerRetention
      : data.summary.avgRevenueRetention

    return (
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-6">
        <SummaryCard
          label={t('retention.totalCohorts')}
          value={formatNumber(data.summary.totalCohorts)}
          subtitle={t('retention.lastMonths', { count: monthsBack })}
          icon={<Users size={28} />}
        />
        <SummaryCard
          label={t('customer.totalCustomers')}
          value={formatNumber(data.summary.totalCustomers)}
          subtitle={t('retention.inAnalyzedCohorts')}
          icon={<Repeat size={28} />}
        />
        <SummaryCard
          label={activeTab === 'revenue' ? t('retention.avgM1RevRetention') : t('retention.avgM1Retention')}
          value={avgRetention?.[1] ? formatPercent(avgRetention[1]) : '-'}
          subtitle={t('retention.returnIn2ndMonth')}
          variant="emerald"
          icon={<TrendingUp size={28} />}
          trend={activeTab === 'retention' ? m1Trend : null}
        />
        {activeTab === 'revenue' && data.summary.totalRevenue != null && (
          <SummaryCard
            label={t('chart.totalRevenue')}
            value={formatCurrency(data.summary.totalRevenue)}
            subtitle={t('retention.m0Revenue')}
            variant="blue"
          />
        )}
        {activeTab === 'retention' && (
          <SummaryCard
            label={t('retention.avgM3Retention')}
            value={avgRetention?.[3] ? formatPercent(avgRetention[3]) : '-'}
            subtitle={t('retention.returnIn4thMonth')}
          />
        )}
      </div>
    )
  }

  // Render content based on active tab
  const renderContent = () => {
    // Show skeleton when loading
    if (activeQuery.isLoading) {
      return <TabSkeleton tab={activeTab} />
    }

    switch (activeTab) {
      case 'retention':
        if (!cohortQuery.data?.cohorts) return null
        return (
          <>
            {renderSummaryCards()}
            <RetentionMatrix
              cohorts={cohortQuery.data.cohorts}
              retentionMonths={retentionMonths}
              type="customer"
            />
            <RetentionLegend />
            {cohortQuery.data.insights && (
              <RetentionInsights
                insights={cohortQuery.data.insights}
                type="customer"
              />
            )}
          </>
        )

      case 'revenue':
        if (!cohortQuery.data?.cohorts) return null
        return (
          <>
            {renderSummaryCards()}
            <RetentionMatrix
              cohorts={cohortQuery.data.cohorts}
              retentionMonths={retentionMonths}
              type="revenue"
            />
            <RetentionLegend />
            {cohortQuery.data.insights && (
              <RetentionInsights
                insights={cohortQuery.data.insights}
                type="revenue"
              />
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

  // Get info text based on active tab
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
      {/* Tab Navigation */}
      <div className="flex flex-wrap gap-2 mb-3">
        {TABS.map((tab) => (
          <TabButton
            key={tab.id}
            tab={tab}
            isActive={activeTab === tab.id}
            onClick={() => setActiveTab(tab.id)}
          />
        ))}
      </div>

      {/* Control Bar */}
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

      {/* Info Banner */}
      <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
        <p className="text-sm text-blue-800">
          <strong>{t('retention.howToRead')}</strong> {getInfoText()}
        </p>
      </div>

      {/* Tab Content */}
      {renderContent()}
    </ChartContainer>
  )
})
