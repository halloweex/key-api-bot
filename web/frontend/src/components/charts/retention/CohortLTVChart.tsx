import { memo, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
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
import { DollarSign, TrendingUp, Users } from 'lucide-react'
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
import { SummaryCard } from './SummaryCard'

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
  const { t } = useTranslation()

  const maxMonth = useMemo(() => {
    if (!data.cohorts.length) return 12
    return data.cohorts[0].cumulativeRevenue?.length
      ? data.cohorts[0].cumulativeRevenue.length
      : 13
  }, [data.cohorts])

  // Transform data for line chart
  const chartData = useMemo(() => {
    const months = Array.from({ length: maxMonth }, (_, i) => i)

    return months.map((monthIndex) => {
      const point: Record<string, number | string> = { month: `M${monthIndex}` }

      data.cohorts.slice(0, 8).forEach((cohort) => {
        if (cohort.cumulativeRevenue && cohort.cumulativeRevenue[monthIndex] !== undefined) {
          const ltv = cohort.customerCount > 0
            ? cohort.cumulativeRevenue[monthIndex] / cohort.customerCount
            : 0
          point[cohort.month] = Math.round(ltv)
        }
      })

      return point
    })
  }, [data.cohorts, maxMonth])

  const visibleCohorts = data.cohorts.slice(0, 8)

  return (
    <div>
      {/* Summary Stats */}
      <div className="grid grid-cols-3 gap-4 mb-6">
        <SummaryCard
          label={t('retention.avgLTV')}
          value={formatCurrency(data.summary.avgLTV)}
          subtitle={t('retention.acrossAllCohorts')}
          variant="blue"
          icon={<DollarSign size={28} />}
        />
        <SummaryCard
          label={t('retention.bestCohort')}
          value={data.summary.bestCohort || '-'}
          subtitle={`LTV: ${formatCurrency(data.summary.bestCohortLTV)}`}
          variant="emerald"
          icon={<TrendingUp size={28} />}
        />
        <SummaryCard
          label={t('retention.cohortsAnalyzed')}
          value={formatNumber(data.cohorts.length)}
          icon={<Users size={28} />}
        />
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
              labelFormatter={(label) => `${t('retention.month')} ${label}`}
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
          <strong>{t('retention.howToRead')}</strong> {t('retention.ltvHowToRead')}
        </p>
      </div>
    </div>
  )
})
