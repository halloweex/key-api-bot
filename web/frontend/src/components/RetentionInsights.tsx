import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { TrendingUp, TrendingDown, DollarSign, Activity, Award, ChevronDown, ChevronUp, Minus } from 'lucide-react'
import { InsightCard } from './InsightCard'
import type { CohortInsights } from '../types/api'
import { formatCurrency } from '../utils/formatters'

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
                insights.retentionTrend.direction === 'improving' ? TrendingUp :
                insights.retentionTrend.direction === 'declining' ? TrendingDown :
                Minus
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
              tone={
                insights.retentionTrend.direction === 'improving' ? 'green' :
                insights.retentionTrend.direction === 'declining' ? 'red' : 'neutral'
              }
            />
          )}

          {/* Revenue Opportunity */}
          {insights.revenueImpact && insights.revenueImpact.monthlyPotential > 0 && (
            <InsightCard
              icon={DollarSign}
              title={t('retention.revenueOpportunity')}
              value={`${formatCurrency(insights.revenueImpact.monthlyPotential)}/mo ${t('retention.potential')}`}
              description={t('retention.insightRevenueImpact', {
                amount: formatCurrency(insights.revenueImpact.monthlyPotential),
                bestM1: insights.revenueImpact.bestM1,
              })}
              tone="amber"
            />
          )}

          {/* Decay Profile */}
          {insights.decayAnalysis && (
            <InsightCard
              icon={Activity}
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
              tone={
                !insights.decayAnalysis.halfLifeMonth ? 'green' :
                insights.decayAnalysis.halfLifeMonth <= 2 ? 'red' :
                insights.decayAnalysis.halfLifeMonth <= 3 ? 'amber' : 'green'
              }
            />
          )}

          {/* Best Cohort */}
          {insights.cohortQualityTrend && (
            <InsightCard
              icon={Award}
              title={t('retention.bestCohort')}
              value={insights.cohortQualityTrend.bestCohort.month}
              description={t('retention.insightBestCohort', {
                month: insights.cohortQualityTrend.bestCohort.month,
                score: insights.cohortQualityTrend.bestCohort.score,
              })}
              tone="green"
            />
          )}
        </div>
      )}
    </div>
  )
})
