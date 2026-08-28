import { Users, TrendingUp, Repeat } from 'lucide-react'
import { useTranslation } from 'react-i18next'
import type { EnhancedCohortRetentionResponse } from '../types/api'
import { formatNumber, formatPercent, formatCurrency } from '../utils/formatters'
import { MetricCard } from './MetricCard'
import { Badge } from './Badge'
import type { TabId } from './cohortTabsConfig'
import { TileGrid } from './TileGrid'
import { Wrapper } from './Wrapper'

interface CohortSummaryCardsProps {
  data: EnhancedCohortRetentionResponse
  activeTab: TabId
  monthsBack: number
  m1Trend: number | null
}

export function CohortSummaryCards({ data, activeTab, monthsBack, m1Trend }: CohortSummaryCardsProps) {
  const { t } = useTranslation()

  if (!data.summary) return null

  const avgRetention =
    activeTab === 'retention' ? data.summary.avgCustomerRetention : data.summary.avgRevenueRetention

  const m1TrendBadge =
    activeTab === 'retention' && m1Trend != null && m1Trend !== 0
      ? (
          <Badge tone={m1Trend > 0 ? 'green' : 'red'} shape="square">
            {m1Trend > 0 ? '+' : ''}
            {m1Trend.toFixed(1)}pp
          </Badge>
        )
      : undefined

  return (
    <Wrapper marginBottom="xl"><TileGrid columns={4}>
      <MetricCard
        surface="tile-gradient"
        tone="neutral"
        iconStyle="watermark"
        icon={Users}
        label={t('retention.totalCohorts')}
        value={formatNumber(data.summary.totalCohorts)}
        sub={t('retention.lastMonths', { count: monthsBack })}
      />
      <MetricCard
        surface="tile-gradient"
        tone="neutral"
        iconStyle="watermark"
        icon={Repeat}
        label={t('customer.totalCustomers')}
        value={formatNumber(data.summary.totalCustomers)}
        sub={t('retention.inAnalyzedCohorts')}
      />
      <MetricCard
        surface="tile-gradient"
        tone="green"
        iconStyle="watermark"
        icon={TrendingUp}
        label={activeTab === 'revenue' ? t('retention.avgM1RevRetention') : t('retention.avgM1Retention')}
        value={avgRetention?.[1] ? formatPercent(avgRetention[1]) : '-'}
        sub={t('retention.returnIn2ndMonth')}
        valueExtra={m1TrendBadge}
      />
      {activeTab === 'revenue' && data.summary.totalRevenue != null && (
        <MetricCard
          surface="tile-gradient"
          tone="blue"
          label={t('chart.totalRevenue')}
          value={formatCurrency(data.summary.totalRevenue)}
          sub={t('retention.m0Revenue')}
        />
      )}
      {activeTab === 'retention' && (
        <MetricCard
          surface="tile-gradient"
          tone="neutral"
          label={t('retention.avgM3Retention')}
          value={avgRetention?.[3] ? formatPercent(avgRetention[3]) : '-'}
          sub={t('retention.returnIn4thMonth')}
        />
      )}
    </TileGrid></Wrapper>
  )
}
