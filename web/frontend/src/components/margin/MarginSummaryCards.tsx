import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { CircleDollarSign, TrendingUp, Package, ShieldCheck } from 'lucide-react'
import { useMarginOverview } from '../../hooks'
import { formatCurrency, formatPercent, formatNumber } from '../../utils/formatters'
import { SkeletonChart } from '../ui'
import { MetricCard } from '../MetricCard'

export const MarginSummaryCards = memo(function MarginSummaryCards() {
  const { t } = useTranslation()
  const { data, isLoading } = useMarginOverview()

  if (isLoading) return <SkeletonChart />
  if (!data) return null

  return (
    <section aria-label={t('margin.overview')}>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
        <MetricCard
          surface="tile-gradient"
          tone="blue"
          icon={<CircleDollarSign className="w-5 h-5" />}
          label={t('margin.costedRevenue')}
          value={formatCurrency(data.costed_revenue)}
          sub={`${t('margin.totalRevenue')}: ${formatCurrency(data.total_revenue)}`}
        />
        <MetricCard
          surface="tile-gradient"
          tone="orange"
          icon={<Package className="w-5 h-5" />}
          label={t('margin.cogs')}
          value={formatCurrency(data.cogs)}
          sub={`${formatNumber(data.total_units)} ${t('margin.units')}`}
        />
        <MetricCard
          surface="tile-gradient"
          tone="green"
          icon={<TrendingUp className="w-5 h-5" />}
          label={t('margin.grossProfit')}
          value={formatCurrency(data.profit)}
          sub={`${t('margin.margin')}: ${formatPercent(data.margin_pct)}`}
        />
        <MetricCard
          surface="tile-gradient"
          tone="purple"
          icon={<ShieldCheck className="w-5 h-5" />}
          label={t('margin.costCoverage')}
          value={formatPercent(data.coverage_pct)}
          sub={`${data.skus_with_cost}/${data.total_skus} ${t('margin.skus')}`}
        />
      </div>
    </section>
  )
})
