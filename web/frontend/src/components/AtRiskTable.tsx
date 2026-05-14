import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { AlertTriangle, Users, Clock, TrendingUp } from 'lucide-react'
import type { AtRiskResponse } from '../types/api'
import { formatNumber, formatCurrency, formatPercent } from '../utils/formatters'
import { MetricCard } from './MetricCard'
import { InfoBanner } from './InfoBanner'

// ─── Types ───────────────────────────────────────────────────────────────────

interface AtRiskTableProps {
  data: AtRiskResponse
}

// ─── Color helpers ───────────────────────────────────────────────────────────

function getRiskColor(percent: number): string {
  if (percent >= 50) return 'bg-red-100 text-red-800'
  if (percent >= 30) return 'bg-orange-100 text-orange-800'
  if (percent >= 20) return 'bg-amber-100 text-amber-800'
  return 'bg-emerald-100 text-emerald-800'
}

function getRiskBadge(percent: number, t: (key: string) => string): { label: string; color: string } {
  if (percent >= 50) return { label: t('retention.riskCritical'), color: 'bg-red-500 text-white' }
  if (percent >= 30) return { label: t('retention.riskHigh'), color: 'bg-orange-500 text-white' }
  if (percent >= 20) return { label: t('retention.riskMedium'), color: 'bg-amber-500 text-white' }
  return { label: t('retention.riskLow'), color: 'bg-emerald-500 text-white' }
}

// ─── Component ───────────────────────────────────────────────────────────────

export const AtRiskTable = memo(function AtRiskTable({ data }: AtRiskTableProps) {
  const { t } = useTranslation()
  const totalAtRiskRevenue = data.cohorts.reduce((sum, c) => sum + c.atRiskRevenue, 0)

  return (
    <div>
      {/* Summary Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        <MetricCard
          surface="tile-gradient"
          tone="red"
          iconStyle="watermark"
          icon={<AlertTriangle size={28} />}
          label={t('retention.atRiskCustomers')}
          value={formatNumber(data.summary.totalAtRisk)}
          sub={`${formatPercent(data.summary.overallAtRiskPct)} ${t('retention.atRiskOfTotal')}`}
        />
        <MetricCard
          surface="tile-gradient"
          tone="red"
          iconStyle="watermark"
          icon={<Users size={28} />}
          label={t('retention.churnedCustomers')}
          value={formatNumber(data.summary.totalChurned)}
          sub={`${formatPercent(data.summary.churnPct)} ${t('retention.atRiskOfTotal')}`}
        />
        <MetricCard
          surface="tile-gradient"
          tone="orange"
          iconStyle="watermark"
          icon={<TrendingUp size={28} />}
          label={t('retention.atRiskRevenue')}
          value={formatCurrency(totalAtRiskRevenue)}
          sub={t('retention.historicalValue')}
        />
        <MetricCard
          surface="tile-gradient"
          tone="blue"
          iconStyle="watermark"
          icon={<Clock size={28} />}
          label={t('retention.threshold')}
          value={`${data.daysThreshold} ${t('retention.days')}`}
          sub={t('retention.sinceLastPurchase')}
        />
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-200">
              <th className="text-left py-2 px-3 font-medium text-slate-700">{t('retention.cohort')}</th>
              <th className="text-center py-2 px-3 font-medium text-slate-700">{t('retention.totalCol')}</th>
              <th className="text-center py-2 px-3 font-medium text-slate-700">{t('retention.atRiskCol')}</th>
              <th className="text-center py-2 px-3 font-medium text-slate-700">{t('retention.riskPct')}</th>
              <th className="text-center py-2 px-3 font-medium text-slate-700">{t('retention.atRiskRevenue')}</th>
              <th className="text-center py-2 px-3 font-medium text-slate-700">{t('retention.avgOrders')}</th>
              <th className="text-center py-2 px-3 font-medium text-slate-700">{t('retention.status')}</th>
            </tr>
          </thead>
          <tbody>
            {data.cohorts.map((cohort) => {
              const badge = getRiskBadge(cohort.atRiskPct, t)
              return (
                <tr key={cohort.cohort} className="border-b border-slate-100 hover:bg-slate-50">
                  <td className="py-2 px-3 font-medium text-slate-700">{cohort.cohort}</td>
                  <td className="text-center py-2 px-3 text-slate-600">
                    {formatNumber(cohort.totalCustomers)}
                  </td>
                  <td className="text-center py-2 px-3 text-red-600 font-medium">
                    {formatNumber(cohort.atRiskCount)}
                  </td>
                  <td className="text-center py-2 px-3">
                    <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${getRiskColor(cohort.atRiskPct)}`}>
                      {formatPercent(cohort.atRiskPct)}
                    </span>
                  </td>
                  <td className="text-center py-2 px-3 text-slate-600">
                    {formatCurrency(cohort.atRiskRevenue)}
                  </td>
                  <td className="text-center py-2 px-3 text-slate-600">
                    {cohort.avgOrdersAtRisk.toFixed(1)}
                  </td>
                  <td className="text-center py-2 px-3">
                    <span className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${badge.color}`}>
                      {badge.label}
                    </span>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {/* Recommendations */}
      <div className="mt-4 space-y-2">
        {data.summary.overallAtRiskPct >= 40 && (
          <div className="p-3 bg-red-50 border border-red-200 rounded-lg">
            <p className="text-sm text-red-800">
              <strong>{t('retention.actionRequired')}</strong> {formatPercent(data.summary.overallAtRiskPct)} {t('retention.actionDesc')}
            </p>
          </div>
        )}
        <InfoBanner>
          <strong>{t('retention.tip')}</strong> {t('retention.tipDesc')}
        </InfoBanner>
      </div>
    </div>
  )
})
