import { memo, useMemo } from 'react'
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import type { CohortLTVResponse } from '../../../types/api'
import { formatCurrency, formatNumber } from '../../../utils/formatters'
import {
  CHART_DIMENSIONS,
  TOOLTIP_STYLE,
  TOOLTIP_LABEL_STYLE,
  GRID_PROPS,
  X_AXIS_PROPS,
  Y_AXIS_PROPS,
  formatAxisK,
} from '../config'

// ─── Types ───────────────────────────────────────────────────────────────────

interface CohortLTVChartProps {
  data: CohortLTVResponse
}

// ─── Colors ──────────────────────────────────────────────────────────────────

const COHORT_COLORS = [
  '#2563EB', // blue-600
  '#7C3AED', // violet-600
  '#059669', // emerald-600
  '#DC2626', // red-600
  '#D97706', // amber-600
  '#0891B2', // cyan-600
  '#4F46E5', // indigo-600
  '#DB2777', // pink-600
  '#65A30D', // lime-600
  '#0D9488', // teal-600
  '#9333EA', // purple-600
  '#EA580C', // orange-600
]

// ─── Component ───────────────────────────────────────────────────────────────

export const CohortLTVChart = memo(function CohortLTVChart({
  data
}: CohortLTVChartProps) {
  // Transform data for line chart - each month (M0-M12) is a point on X axis
  // Each cohort is a separate line
  const chartData = useMemo(() => {
    const months = Array.from({ length: 13 }, (_, i) => i) // M0 to M12

    return months.map((monthIndex) => {
      const point: Record<string, number | string> = { month: `M${monthIndex}` }

      data.cohorts.slice(0, 8).forEach((cohort) => {
        // Only include data if the cohort has data for this month
        if (cohort.cumulativeRevenue && cohort.cumulativeRevenue[monthIndex] !== undefined) {
          // Calculate LTV (cumulative revenue / customer count)
          const ltv = cohort.customerCount > 0
            ? cohort.cumulativeRevenue[monthIndex] / cohort.customerCount
            : 0
          point[cohort.month] = Math.round(ltv)
        }
      })

      return point
    })
  }, [data.cohorts])

  // Get visible cohorts (first 8 for readability)
  const visibleCohorts = data.cohorts.slice(0, 8)

  return (
    <div>
      {/* Summary Stats */}
      <div className="grid grid-cols-3 gap-4 mb-6">
        <div className="bg-gradient-to-br from-blue-50 to-blue-100/50 border border-blue-200 rounded-xl p-4">
          <p className="text-xs text-slate-600 font-medium">Avg LTV</p>
          <p className="text-xl font-bold text-blue-800">
            {formatCurrency(data.summary.avgLTV)}
          </p>
          <p className="text-xs text-slate-500">across all cohorts</p>
        </div>
        <div className="bg-gradient-to-br from-emerald-50 to-emerald-100/50 border border-emerald-200 rounded-xl p-4">
          <p className="text-xs text-slate-600 font-medium">Best Cohort</p>
          <p className="text-xl font-bold text-emerald-800">
            {data.summary.bestCohort || '-'}
          </p>
          <p className="text-xs text-slate-500">
            LTV: {formatCurrency(data.summary.bestCohortLTV)}
          </p>
        </div>
        <div className="bg-gradient-to-br from-slate-50 to-slate-100/50 border border-slate-200 rounded-xl p-4">
          <p className="text-xs text-slate-600 font-medium">Cohorts Analyzed</p>
          <p className="text-xl font-bold text-slate-800">
            {formatNumber(data.cohorts.length)}
          </p>
        </div>
      </div>

      {/* Chart */}
      <div style={{ height: CHART_DIMENSIONS.height.xl }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={chartData} margin={{ ...CHART_DIMENSIONS.margin.default, right: 30 }}>
            <CartesianGrid {...GRID_PROPS} />
            <XAxis
              dataKey="month"
              {...X_AXIS_PROPS}
            />
            <YAxis
              {...Y_AXIS_PROPS}
              tickFormatter={(v) => formatAxisK(v)}
              label={{ value: 'LTV', angle: -90, position: 'insideLeft', fontSize: 11 }}
            />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              labelStyle={TOOLTIP_LABEL_STYLE}
              formatter={(value, name) => [formatCurrency(typeof value === 'number' ? value : 0), String(name)]}
              labelFormatter={(label) => `Month: ${label}`}
            />
            <Legend
              verticalAlign="top"
              height={36}
              wrapperStyle={{ fontSize: 11 }}
            />
            {visibleCohorts.map((cohort, index) => (
              <Line
                key={cohort.month}
                type="monotone"
                dataKey={cohort.month}
                name={cohort.month}
                stroke={COHORT_COLORS[index % COHORT_COLORS.length]}
                strokeWidth={2}
                dot={false}
                activeDot={{ r: 4 }}
                connectNulls
              />
            ))}
          </LineChart>
        </ResponsiveContainer>
      </div>

      {/* Info */}
      <div className="mt-4 p-3 bg-amber-50 border border-amber-200 rounded-lg">
        <p className="text-sm text-amber-800">
          <strong>How to read:</strong> Each line shows how cumulative LTV (lifetime value per customer)
          grows over time for a specific cohort. Steeper lines = faster value accumulation.
          Compare cohorts to identify which acquisition periods brought the most valuable customers.
        </p>
      </div>
    </div>
  )
})
