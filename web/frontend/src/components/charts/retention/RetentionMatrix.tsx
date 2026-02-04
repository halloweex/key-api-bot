import { memo } from 'react'
import type { EnhancedCohortData } from '../../../types/api'
import { formatNumber } from '../../../utils/formatters'

// ─── Color Scale ─────────────────────────────────────────────────────────────

function getRetentionColor(percent: number | null): string {
  if (percent === null) return 'bg-slate-100'
  if (percent >= 100) return 'bg-emerald-500 text-white'
  if (percent >= 20) return 'bg-emerald-400 text-white'
  if (percent >= 15) return 'bg-emerald-300'
  if (percent >= 10) return 'bg-emerald-200'
  if (percent >= 5) return 'bg-emerald-100'
  if (percent > 0) return 'bg-emerald-50'
  return 'bg-slate-100'
}

// ─── Types ───────────────────────────────────────────────────────────────────

interface RetentionMatrixProps {
  cohorts: EnhancedCohortData[]
  retentionMonths: number
  type: 'customer' | 'revenue'
}

// ─── Component ───────────────────────────────────────────────────────────────

export const RetentionMatrix = memo(function RetentionMatrix({
  cohorts,
  retentionMonths,
  type
}: RetentionMatrixProps) {
  // Column headers (M0, M1, M2, ...)
  const headers = Array.from({ length: retentionMonths + 1 }, (_, i) => `M${i}`)

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200">
            <th className="text-left py-2 px-3 font-semibold text-slate-700 sticky left-0 bg-white">
              Cohort
            </th>
            <th className="text-center py-2 px-2 font-semibold text-slate-700 w-16">
              Size
            </th>
            {headers.map((header) => (
              <th
                key={header}
                className="text-center py-2 px-2 font-semibold text-slate-700 w-14"
                title={header === 'M0' ? 'First purchase month' : `${header.slice(1)} month(s) after first purchase`}
              >
                {header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {cohorts.map((cohort) => {
            const data = type === 'customer' ? cohort.retention : cohort.revenueRetention
            return (
              <tr key={cohort.month} className="border-b border-slate-100 hover:bg-slate-50">
                <td className="py-2 px-3 font-medium text-slate-700 sticky left-0 bg-white">
                  {cohort.month}
                </td>
                <td className="text-center py-2 px-2 text-slate-600">
                  {formatNumber(cohort.size)}
                </td>
                {data?.map((percent, index) => (
                  <td key={index} className="text-center py-1 px-1">
                    <div
                      className={`py-1.5 px-1 rounded text-xs font-medium ${getRetentionColor(percent)}`}
                      title={percent !== null ? `${percent}% ${type === 'customer' ? 'of cohort returned' : 'revenue retention'} in month ${index}` : 'No data yet'}
                    >
                      {percent !== null ? `${percent}%` : '-'}
                    </div>
                  </td>
                ))}
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
})

// ─── Legend ──────────────────────────────────────────────────────────────────

export const RetentionLegend = memo(function RetentionLegend() {
  return (
    <div className="mt-4 flex flex-wrap items-center gap-4 text-xs text-slate-600">
      <span className="font-medium">Retention %:</span>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-50"></div>
        <span>&lt;5%</span>
      </div>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-100"></div>
        <span>5-10%</span>
      </div>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-200"></div>
        <span>10-15%</span>
      </div>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-300"></div>
        <span>15-20%</span>
      </div>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-400"></div>
        <span>20%+</span>
      </div>
    </div>
  )
})
