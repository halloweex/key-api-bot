import { memo } from 'react'
import type { AtRiskResponse } from '../../../types/api'
import { formatNumber, formatCurrency, formatPercent } from '../../../utils/formatters'

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

function getRiskBadge(percent: number): { label: string; color: string } {
  if (percent >= 50) return { label: 'Critical', color: 'bg-red-500 text-white' }
  if (percent >= 30) return { label: 'High', color: 'bg-orange-500 text-white' }
  if (percent >= 20) return { label: 'Medium', color: 'bg-amber-500 text-white' }
  return { label: 'Low', color: 'bg-emerald-500 text-white' }
}

// ─── Component ───────────────────────────────────────────────────────────────

export const AtRiskTable = memo(function AtRiskTable({ data }: AtRiskTableProps) {
  const totalAtRiskRevenue = data.cohorts.reduce((sum, c) => sum + c.atRiskRevenue, 0)

  return (
    <div>
      {/* Summary Stats */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        <div className="bg-gradient-to-br from-red-50 to-red-100/50 border border-red-200 rounded-xl p-4">
          <p className="text-xs text-red-700 font-medium">At-Risk Customers</p>
          <p className="text-xl font-bold text-red-800">
            {formatNumber(data.summary.totalAtRisk)}
          </p>
          <p className="text-xs text-red-600">
            {formatPercent(data.summary.overallAtRiskPct)} of total
          </p>
        </div>
        <div className="bg-gradient-to-br from-slate-50 to-slate-100/50 border border-slate-200 rounded-xl p-4">
          <p className="text-xs text-slate-600 font-medium">Total Customers</p>
          <p className="text-xl font-bold text-slate-800">
            {formatNumber(data.summary.totalCustomers)}
          </p>
          <p className="text-xs text-slate-500">in last 12 months</p>
        </div>
        <div className="bg-gradient-to-br from-amber-50 to-amber-100/50 border border-amber-200 rounded-xl p-4">
          <p className="text-xs text-amber-700 font-medium">At-Risk Revenue</p>
          <p className="text-xl font-bold text-amber-800">
            {formatCurrency(totalAtRiskRevenue)}
          </p>
          <p className="text-xs text-amber-600">historical value</p>
        </div>
        <div className="bg-gradient-to-br from-blue-50 to-blue-100/50 border border-blue-200 rounded-xl p-4">
          <p className="text-xs text-blue-700 font-medium">Threshold</p>
          <p className="text-xl font-bold text-blue-800">
            {data.daysThreshold} days
          </p>
          <p className="text-xs text-blue-600">since last purchase</p>
        </div>
      </div>

      {/* Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-200">
              <th className="text-left py-2 px-3 font-semibold text-slate-700">Cohort</th>
              <th className="text-center py-2 px-3 font-semibold text-slate-700">Total</th>
              <th className="text-center py-2 px-3 font-semibold text-slate-700">At Risk</th>
              <th className="text-center py-2 px-3 font-semibold text-slate-700">Risk %</th>
              <th className="text-center py-2 px-3 font-semibold text-slate-700">At-Risk Revenue</th>
              <th className="text-center py-2 px-3 font-semibold text-slate-700">Avg Orders</th>
              <th className="text-center py-2 px-3 font-semibold text-slate-700">Status</th>
            </tr>
          </thead>
          <tbody>
            {data.cohorts.map((cohort) => {
              const badge = getRiskBadge(cohort.atRiskPct)
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
              <strong>Action Required:</strong> {formatPercent(data.summary.overallAtRiskPct)} of customers are at risk.
              Consider launching a win-back email campaign with special offers for inactive customers.
            </p>
          </div>
        )}
        <div className="p-3 bg-blue-50 border border-blue-200 rounded-lg">
          <p className="text-sm text-blue-800">
            <strong>Tip:</strong> Focus on cohorts with high at-risk percentages but also high historical revenue -
            these customers have demonstrated purchase potential and may respond well to re-engagement.
          </p>
        </div>
      </div>
    </div>
  )
})
