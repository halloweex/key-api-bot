import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { CircleDollarSign, TrendingUp, Package, ShieldCheck } from 'lucide-react'
import { useMarginOverview } from '../../hooks'
import { formatCurrency, formatPercent, formatNumber } from '../../utils/formatters'
import { SkeletonChart } from '../ui'

// ─── Metric Card ─────────────────────────────────────────────────────────────

interface MetricCardProps {
  icon: React.ReactNode
  label: string
  value: string
  subtitle?: string
  colorClass: string
  bgClass: string
  iconBgClass: string
}

const MetricCard = memo(function MetricCard({ icon, label, value, subtitle, colorClass, bgClass, iconBgClass }: MetricCardProps) {
  return (
    <div className={`rounded-xl p-4 border ${bgClass}`}>
      <div className="flex items-start gap-3">
        <div className={`p-2 lg:p-2.5 rounded-lg ${iconBgClass} ${colorClass}`}>
          {icon}
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-xs text-slate-600 font-medium">{label}</p>
          <p className={`text-xl font-bold truncate ${colorClass}`}>{value}</p>
          {subtitle && <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p>}
        </div>
      </div>
    </div>
  )
})

// ─── Component ───────────────────────────────────────────────────────────────

export const MarginSummaryCards = memo(function MarginSummaryCards() {
  const { t } = useTranslation()
  const { data, isLoading } = useMarginOverview()

  if (isLoading) return <SkeletonChart />
  if (!data) return null

  return (
    <section aria-label={t('margin.overview')}>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
        <MetricCard
          icon={<CircleDollarSign className="w-5 h-5" />}
          label={t('margin.costedRevenue')}
          value={formatCurrency(data.costed_revenue)}
          subtitle={`${t('margin.totalRevenue')}: ${formatCurrency(data.total_revenue)}`}
          colorClass="text-blue-600"
          bgClass="bg-gradient-to-br from-blue-100 to-blue-50 border-blue-200"
          iconBgClass="bg-blue-200/60"
        />
        <MetricCard
          icon={<Package className="w-5 h-5" />}
          label={t('margin.cogs')}
          value={formatCurrency(data.cogs)}
          subtitle={`${formatNumber(data.total_units)} ${t('margin.units')}`}
          colorClass="text-orange-600"
          bgClass="bg-gradient-to-br from-orange-100 to-orange-50 border-orange-200"
          iconBgClass="bg-orange-200/60"
        />
        <MetricCard
          icon={<TrendingUp className="w-5 h-5" />}
          label={t('margin.grossProfit')}
          value={formatCurrency(data.profit)}
          subtitle={`${t('margin.margin')}: ${formatPercent(data.margin_pct)}`}
          colorClass="text-green-600"
          bgClass="bg-gradient-to-br from-green-100 to-green-50 border-green-200"
          iconBgClass="bg-green-200/60"
        />
        <MetricCard
          icon={<ShieldCheck className="w-5 h-5" />}
          label={t('margin.costCoverage')}
          value={formatPercent(data.coverage_pct)}
          subtitle={`${data.skus_with_cost}/${data.total_skus} ${t('margin.skus')}`}
          colorClass="text-purple-600"
          bgClass="bg-gradient-to-br from-purple-100 to-purple-50 border-purple-200"
          iconBgClass="bg-purple-200/60"
        />
      </div>
    </section>
  )
})
