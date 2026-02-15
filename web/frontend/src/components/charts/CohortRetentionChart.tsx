import { memo, useState, useMemo } from 'react'
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
  { id: 'retention', label: 'Customer Retention', shortLabel: 'Retention' },
  { id: 'revenue', label: 'Revenue Retention', shortLabel: 'Revenue' },
  { id: 'timing', label: 'Purchase Timing', shortLabel: 'Timing' },
  { id: 'ltv', label: 'Lifetime Value', shortLabel: 'LTV' },
  { id: 'at-risk', label: 'At-Risk Customers', shortLabel: 'At-Risk' },
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
      <span className="hidden sm:inline">{tab.label}</span>
      <span className="sm:hidden">{tab.shortLabel}</span>
    </button>
  )
})

// ─── Component ───────────────────────────────────────────────────────────────

export const CohortRetentionChart = memo(function CohortRetentionChart() {
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
            label="Total Cohorts"
            value={formatNumber(data.summary.totalCohorts)}
            subtitle={`Last ${monthsBack} months`}
          />
          <SummaryCard
            label="Total Customers"
            value={formatNumber(data.summary.totalCustomers)}
            subtitle="In analyzed cohorts"
          />
          <SummaryCard
            label={`Avg M1 ${activeTab === 'revenue' ? 'Rev' : ''} Retention`}
            value={avgRetention?.[1] ? formatPercent(avgRetention[1]) : '-'}
            subtitle="Return in 2nd month"
            variant="emerald"
          />
          {activeTab === 'revenue' && data.summary.totalRevenue && (
            <SummaryCard
              label="Total Revenue"
              value={formatCurrency(data.summary.totalRevenue)}
              subtitle="M0 revenue"
              variant="blue"
            />
          )}
          {activeTab === 'retention' && (
            <SummaryCard
              label="Avg M3 Retention"
              value={avgRetention?.[3] ? formatPercent(avgRetention[3]) : '-'}
              subtitle="Return in 4th month"
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
        return 'Each row is a cohort of customers who made their first purchase in that month. The percentages show what % returned to purchase again in subsequent months.'
      case 'revenue':
        return 'Shows revenue retention: what % of original cohort revenue is generated in each subsequent month. Useful for understanding revenue sustainability.'
      case 'timing':
        return 'Analyzes how long it takes customers to make their second purchase. Helps optimize re-engagement timing and email automation triggers.'
      case 'ltv':
        return 'Tracks cumulative lifetime value per customer for each cohort over time. Compare cohorts to identify which acquisition periods brought valuable customers.'
      case 'at-risk':
        return 'Identifies customers who haven\'t purchased in 90+ days, grouped by their acquisition cohort. Focus on high-value at-risk customers for win-back campaigns.'
      default:
        return ''
    }
  }

  return (
    <ChartContainer
      title="Cohort Analysis"
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
          <strong>How to read:</strong> {getInfoText()}
        </p>
      </div>

      {/* Tab Content */}
      {renderContent()}
    </ChartContainer>
  )
})
