import { memo, useState, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
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
  AtRiskTable
} from './retention'

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

// ─── Summary Card ────────────────────────────────────────────────────────────

interface SummaryCardProps {
  label: string
  value: string
  subtitle?: string
  variant?: 'default' | 'emerald' | 'blue' | 'amber'
}

const SummaryCard = memo(function SummaryCard({
  label,
  value,
  subtitle,
  variant = 'default'
}: SummaryCardProps) {
  const variantStyles = {
    default: 'from-slate-100 to-slate-50 border-slate-200',
    emerald: 'from-emerald-50 to-emerald-100/50 border-emerald-200',
    blue: 'from-blue-50 to-blue-100/50 border-blue-200',
    amber: 'from-amber-50 to-amber-100/50 border-amber-200',
  }
  const valueStyles = {
    default: 'text-slate-800',
    emerald: 'text-emerald-800',
    blue: 'text-blue-800',
    amber: 'text-amber-800',
  }

  return (
    <div className={`bg-gradient-to-br ${variantStyles[variant]} border rounded-xl p-4`}>
      <p className="text-xs text-slate-600 font-medium">{label}</p>
      <p className={`text-xl font-bold ${valueStyles[variant]}`}>{value}</p>
      {subtitle && (
        <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p>
      )}
    </div>
  )
})

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

// ─── Component ───────────────────────────────────────────────────────────────

export const CohortRetentionChart = memo(function CohortRetentionChart() {
  const { t } = useTranslation()
  const [activeTab, setActiveTab] = useState<TabId>('retention')
  const [monthsBack] = useState(12)
  const [retentionMonths] = useState(6)

  // Fetch data for all tabs
  const cohortQuery = useCohortRetention(monthsBack, retentionMonths, true)
  const timingQuery = usePurchaseTiming(monthsBack)
  const ltvQuery = useCohortLTV(monthsBack)
  const atRiskQuery = useAtRiskCustomers(90)

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

  // Summary cards based on active tab
  const renderSummaryCards = () => {
    if (activeTab === 'retention' || activeTab === 'revenue') {
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
          />
          <SummaryCard
            label={t('customer.totalCustomers')}
            value={formatNumber(data.summary.totalCustomers)}
            subtitle={t('retention.inAnalyzedCohorts')}
          />
          <SummaryCard
            label={activeTab === 'revenue' ? t('retention.avgM1RevRetention') : `Avg M1 Retention`}
            value={avgRetention?.[1] ? formatPercent(avgRetention[1]) : '-'}
            subtitle={t('retention.returnIn2ndMonth')}
            variant="emerald"
          />
          {activeTab === 'revenue' && data.summary.totalRevenue && (
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
    return null // Other tabs have their own summary cards
  }

  // Render content based on active tab
  const renderContent = () => {
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
      isLoading={activeQuery.isLoading}
      error={activeQuery.error as Error | null}
      onRetry={activeQuery.refetch}
      isEmpty={isEmpty}
      height="auto"
      ariaLabel="Cohort analysis with retention, revenue, timing, LTV, and at-risk customer views"
    >
      {/* Tab Navigation */}
      <div className="flex flex-wrap gap-2 mb-4">
        {TABS.map((tab) => (
          <TabButton
            key={tab.id}
            tab={tab}
            isActive={activeTab === tab.id}
            onClick={() => setActiveTab(tab.id)}
          />
        ))}
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
