import { memo, useState } from 'react'
import { ChartContainer } from './ChartContainer'
import { useCohortRetention } from '../../hooks'
import { formatNumber, formatPercent } from '../../utils/formatters'

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

// ─── Summary Card ────────────────────────────────────────────────────────────

interface SummaryCardProps {
  label: string
  value: string
  subtitle?: string
}

const SummaryCard = memo(function SummaryCard({
  label,
  value,
  subtitle
}: SummaryCardProps) {
  return (
    <div className="bg-gradient-to-br from-slate-100 to-slate-50 border border-slate-200 rounded-xl p-4">
      <p className="text-xs text-slate-600 font-medium">{label}</p>
      <p className="text-xl font-bold text-slate-800">{value}</p>
      {subtitle && (
        <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p>
      )}
    </div>
  )
})

// ─── Component ───────────────────────────────────────────────────────────────

export const CohortRetentionChart = memo(function CohortRetentionChart() {
  const [monthsBack] = useState(12)
  const [retentionMonths] = useState(6)

  const { data, isLoading, error, refetch } = useCohortRetention(monthsBack, retentionMonths)

  const isEmpty = !isLoading && (!data?.cohorts || data.cohorts.length === 0)

  // Column headers (M0, M1, M2, ...)
  const headers = Array.from({ length: retentionMonths + 1 }, (_, i) => `M${i}`)

  return (
    <ChartContainer
      title="Cohort Retention Analysis"
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xl"
      ariaLabel="Cohort retention matrix showing customer return rates over time"
    >
      {/* Info Banner */}
      <div className="mb-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
        <p className="text-sm text-blue-800">
          <strong>How to read:</strong> Each row is a cohort of customers who made their first purchase in that month.
          The percentages show what % returned to purchase again in subsequent months (M0 = first month, M1 = second month, etc.).
        </p>
      </div>

      {/* Summary Cards */}
      {data?.summary && (
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
            label="Avg M1 Retention"
            value={data.summary.avgRetention[1] ? formatPercent(data.summary.avgRetention[1]) : '-'}
            subtitle="Return in 2nd month"
          />
          <SummaryCard
            label="Avg M3 Retention"
            value={data.summary.avgRetention[3] ? formatPercent(data.summary.avgRetention[3]) : '-'}
            subtitle="Return in 4th month"
          />
        </div>
      )}

      {/* Retention Matrix */}
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
            {data?.cohorts.map((cohort) => (
              <tr key={cohort.month} className="border-b border-slate-100 hover:bg-slate-50">
                <td className="py-2 px-3 font-medium text-slate-700 sticky left-0 bg-white">
                  {cohort.month}
                </td>
                <td className="text-center py-2 px-2 text-slate-600">
                  {formatNumber(cohort.size)}
                </td>
                {cohort.retention.map((percent, index) => (
                  <td key={index} className="text-center py-1 px-1">
                    <div
                      className={`py-1.5 px-1 rounded text-xs font-medium ${getRetentionColor(percent)}`}
                      title={percent !== null ? `${percent}% of cohort returned in month ${index}` : 'No data yet'}
                    >
                      {percent !== null ? `${percent}%` : '-'}
                    </div>
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Legend */}
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
    </ChartContainer>
  )
})
