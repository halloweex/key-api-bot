import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { TrendingUp, TrendingDown, DollarSign, Activity, Award, ChevronDown, ChevronUp, Minus } from 'lucide-react'
import type { CohortInsights } from '../../../types/api'
import { formatCurrency } from '../../../utils/formatters'

interface RetentionInsightsProps {
  insights: CohortInsights
  type: 'customer' | 'revenue'
}

export const RetentionInsights = memo(function RetentionInsights({ insights }: RetentionInsightsProps) {
  const { t } = useTranslation()
  const [expanded, setExpanded] = useState(true)

  const hasAny = insights.retentionTrend || insights.revenueImpact ||
    insights.decayAnalysis || insights.cohortQualityTrend

  if (!hasAny) return null

  return (
    <div className="mt-4">
      <button
        onClick={() => setExpanded(!expanded)}
        className="flex items-center gap-1.5 text-sm font-semibold text-slate-700 hover:text-slate-900 transition-colors mb-2"
      >
        {expanded ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
        {t('retention.keyInsights')}
      </button>

      {expanded && (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          {/* Retention Trend */}
          {insights.retentionTrend && (
            <InsightCard
              icon={
                insights.retentionTrend.direction === 'improving' ? <TrendingUp size={18} /> :
                insights.retentionTrend.direction === 'declining' ? <TrendingDown size={18} /> :
                <Minus size={18} />
              }
              title={t('retention.retentionTrend')}
              value={
                insights.retentionTrend.direction === 'stable'
                  ? '~0pp'
                  : `${insights.retentionTrend.delta > 0 ? '+' : ''}${insights.retentionTrend.delta}pp`
              }
              description={
                insights.retentionTrend.direction === 'improving'
                  ? t('retention.insightTrendImproving', { delta: Math.abs(insights.retentionTrend.delta) })
                  : insights.retentionTrend.direction === 'declining'
                  ? t('retention.insightTrendDeclining', { delta: Math.abs(insights.retentionTrend.delta) })
                  : t('retention.insightTrendStable')
              }
              color={
                insights.retentionTrend.direction === 'improving' ? 'green' :
                insights.retentionTrend.direction === 'declining' ? 'red' : 'slate'
              }
            />
          )}

          {/* Revenue Opportunity */}
          {insights.revenueImpact && insights.revenueImpact.monthlyPotential > 0 && (
            <InsightCard
              icon={<DollarSign size={18} />}
              title={t('retention.revenueOpportunity')}
              value={`${formatCurrency(insights.revenueImpact.monthlyPotential)}/mo ${t('retention.potential')}`}
              description={t('retention.insightRevenueImpact', {
                amount: formatCurrency(insights.revenueImpact.monthlyPotential),
                bestM1: insights.revenueImpact.bestM1,
              })}
              color="amber"
            />
          )}

          {/* Decay Profile */}
          {insights.decayAnalysis && (
            <InsightCard
              icon={<Activity size={18} />}
              title={t('retention.decayProfile')}
              value={
                insights.decayAnalysis.halfLifeMonth
                  ? `${t('retention.halfLife')}: M${insights.decayAnalysis.halfLifeMonth}`
                  : t('retention.insightDecayNoHalfLife')
              }
              description={
                insights.decayAnalysis.stabilizationMonth && insights.decayAnalysis.terminalRetention != null
                  ? t('retention.insightDecayStabilizes', {
                      month: insights.decayAnalysis.stabilizationMonth,
                      rate: insights.decayAnalysis.terminalRetention,
                    })
                  : insights.decayAnalysis.halfLifeMonth
                  ? t('retention.insightDecayHalfLife', { month: insights.decayAnalysis.halfLifeMonth })
                  : t('retention.insightDecayNoHalfLife')
              }
              subtext={`${t('retention.insightDrop')}: ${insights.decayAnalysis.m1ToM3Drop}pp`}
              color={
                !insights.decayAnalysis.halfLifeMonth ? 'green' :
                insights.decayAnalysis.halfLifeMonth <= 2 ? 'red' :
                insights.decayAnalysis.halfLifeMonth <= 3 ? 'amber' : 'green'
              }
            />
          )}

          {/* Best Cohort */}
          {insights.cohortQualityTrend && (
            <InsightCard
              icon={<Award size={18} />}
              title={t('retention.bestCohort')}
              value={insights.cohortQualityTrend.bestCohort.month}
              description={t('retention.insightBestCohort', {
                month: insights.cohortQualityTrend.bestCohort.month,
                score: insights.cohortQualityTrend.bestCohort.score,
              })}
              color="emerald"
            />
          )}
        </div>
      )}
    </div>
  )
})

// ─── Insight Card ──────────────────────────────────────────────────────────

interface InsightCardProps {
  icon: React.ReactNode
  title: string
  value: string
  description: string
  subtext?: string
  color: 'green' | 'red' | 'amber' | 'slate' | 'emerald'
}

const colorStyles = {
  green: {
    bg: 'bg-emerald-50 border-emerald-200',
    icon: 'text-emerald-600',
    value: 'text-emerald-700',
  },
  red: {
    bg: 'bg-red-50 border-red-200',
    icon: 'text-red-600',
    value: 'text-red-700',
  },
  amber: {
    bg: 'bg-amber-50 border-amber-200',
    icon: 'text-amber-600',
    value: 'text-amber-700',
  },
  slate: {
    bg: 'bg-slate-50 border-slate-200',
    icon: 'text-slate-500',
    value: 'text-slate-700',
  },
  emerald: {
    bg: 'bg-emerald-50 border-emerald-200',
    icon: 'text-emerald-600',
    value: 'text-emerald-700',
  },
}

const InsightCard = memo(function InsightCard({ icon, title, value, description, subtext, color }: InsightCardProps) {
  const styles = colorStyles[color]
  return (
    <div className={`${styles.bg} border rounded-lg p-3 flex gap-3`}>
      <div className={`${styles.icon} mt-0.5 shrink-0`}>
        {icon}
      </div>
      <div className="min-w-0">
        <p className="text-xs font-medium text-slate-500">{title}</p>
        <p className={`text-sm font-bold ${styles.value}`}>{value}</p>
        <p className="text-xs text-slate-600 mt-0.5">{description}</p>
        {subtext && (
          <p className="text-xs text-slate-400 mt-0.5">{subtext}</p>
        )}
      </div>
    </div>
  )
})
