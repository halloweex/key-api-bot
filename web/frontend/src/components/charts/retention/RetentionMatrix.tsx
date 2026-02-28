import { memo, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import type { EnhancedCohortData } from '../../../types/api'
import { formatNumber } from '../../../utils/formatters'

// ─── Color Scale (8-level emerald) ───────────────────────────────────────────

function getRetentionColor(percent: number | null): string {
  if (percent === null) return 'bg-slate-100'
  if (percent >= 50) return 'bg-emerald-700 text-white'
  if (percent >= 30) return 'bg-emerald-600 text-white'
  if (percent >= 20) return 'bg-emerald-500 text-white'
  if (percent >= 15) return 'bg-emerald-400 text-white'
  if (percent >= 10) return 'bg-emerald-300'
  if (percent >= 5) return 'bg-emerald-200'
  if (percent > 0) return 'bg-emerald-100'
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
  const { t } = useTranslation()
  const headers = Array.from({ length: retentionMonths + 1 }, (_, i) => `M${i}`)

  const maxSize = useMemo(
    () => Math.max(...cohorts.map(c => c.size), 1),
    [cohorts]
  )

  // Weighted average per column
  const weightedAvg = useMemo(() => {
    const avg: (number | null)[] = []
    for (let m = 0; m <= retentionMonths; m++) {
      let wSum = 0
      let wTotal = 0
      for (const c of cohorts) {
        const data = type === 'customer' ? c.retention : c.revenueRetention
        const pct = data?.[m]
        if (pct != null) {
          wSum += pct * c.size
          wTotal += c.size
        }
      }
      avg.push(wTotal > 0 ? Math.round(wSum / wTotal * 10) / 10 : null)
    }
    return avg
  }, [cohorts, retentionMonths, type])

  // Best/worst per column (skip M0)
  const columnExtremes = useMemo(() => {
    const best: Record<number, number> = {}
    const worst: Record<number, number> = {}
    for (let m = 1; m <= retentionMonths; m++) {
      let hi = -Infinity
      let lo = Infinity
      let hasData = false
      for (const c of cohorts) {
        const data = type === 'customer' ? c.retention : c.revenueRetention
        const pct = data?.[m]
        if (pct != null) {
          hasData = true
          if (pct > hi) hi = pct
          if (pct < lo) lo = pct
        }
      }
      if (hasData && hi !== lo) {
        best[m] = hi
        worst[m] = lo
      }
    }
    return { best, worst }
  }, [cohorts, retentionMonths, type])

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200">
            <th className="text-left py-2 px-3 font-medium text-slate-700 sticky left-0 bg-white z-10 w-[72px]">
              {t('retention.cohort')}
            </th>
            <th className="text-center py-2 px-2 font-medium text-slate-700 w-16">
              {t('retention.size')}
            </th>
            {headers.map((header, i) => (
              <th
                key={header}
                className="text-center py-2 px-2 font-medium text-slate-700 w-14"
                title={i === 0 ? t('retention.firstPurchaseMonth') : t('retention.monthsAfter', { count: i })}
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
                <td className="py-2 px-3 font-medium text-slate-700 sticky left-0 bg-white z-10">
                  {cohort.month}
                </td>
                <td className="text-center py-2 px-2 text-slate-600">
                  <div>
                    {formatNumber(cohort.size)}
                    <div
                      className="h-1 rounded-full bg-emerald-300 mt-0.5"
                      style={{ width: `${(cohort.size / maxSize) * 100}%` }}
                    />
                  </div>
                </td>
                {data?.map((percent, index) => {
                  const isBest = index > 0 && percent != null && columnExtremes.best[index] === percent
                  const isWorst = index > 0 && percent != null && columnExtremes.worst[index] === percent
                  const absCount = percent != null ? Math.round(percent / 100 * cohort.size) : null
                  const revenue = cohort.revenue?.[index]
                  const titleParts = []
                  if (percent != null) {
                    titleParts.push(`${percent}% ${type === 'customer' ? 'retained' : 'revenue retention'}`)
                    if (absCount != null) titleParts.push(`${formatNumber(absCount)} customers`)
                    if (revenue != null && revenue > 0) titleParts.push(`₴${formatNumber(Math.round(revenue))} revenue`)
                  } else {
                    titleParts.push('No data yet')
                  }

                  return (
                    <td key={index} className="text-center py-1 px-1">
                      <div
                        className={`py-1.5 px-1 rounded text-xs font-medium ${getRetentionColor(percent)} ${
                          isBest ? 'ring-2 ring-emerald-400' : isWorst ? 'ring-2 ring-red-300' : ''
                        }`}
                        title={titleParts.join('\n')}
                      >
                        {percent !== null ? `${percent}%` : '-'}
                      </div>
                    </td>
                  )
                })}
              </tr>
            )
          })}
        </tbody>
        <tfoot>
          <tr className="bg-slate-50 border-t-2 border-slate-300">
            <td className="py-2 px-3 font-bold text-slate-700 sticky left-0 bg-slate-50 z-10">
              Avg
            </td>
            <td className="text-center py-2 px-2 text-slate-500 text-xs">
              {t('retention.retentionPct')}
            </td>
            {weightedAvg.map((pct, index) => (
              <td key={index} className="text-center py-1 px-1">
                <div className={`py-1.5 px-1 rounded text-xs font-bold ${getRetentionColor(pct)}`}>
                  {pct !== null ? `${pct}%` : '-'}
                </div>
              </td>
            ))}
          </tr>
        </tfoot>
      </table>
    </div>
  )
})

// ─── Legend ──────────────────────────────────────────────────────────────────

export const RetentionLegend = memo(function RetentionLegend() {
  const { t } = useTranslation()
  return (
    <div className="mt-4 flex flex-wrap items-center gap-4 text-xs text-slate-600">
      <span className="font-medium">{t('retention.retentionPct')}:</span>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-100"></div>
        <span>&lt;5%</span>
      </div>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-200"></div>
        <span>5-10%</span>
      </div>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-300"></div>
        <span>10-15%</span>
      </div>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-400"></div>
        <span>15-20%</span>
      </div>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-500"></div>
        <span>20-30%</span>
      </div>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-600"></div>
        <span>30-50%</span>
      </div>
      <div className="flex items-center gap-1">
        <div className="w-4 h-4 rounded bg-emerald-700"></div>
        <span>50%+</span>
      </div>
    </div>
  )
})
