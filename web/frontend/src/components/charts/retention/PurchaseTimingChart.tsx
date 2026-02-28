import { memo, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell
} from 'recharts'
import { Users, Clock, TrendingUp } from 'lucide-react'
import type { PurchaseTimingResponse } from '../../../types/api'
import { formatNumber, formatPercent } from '../../../utils/formatters'
import {
  CHART_DIMENSIONS,
  TOOLTIP_STYLE,
  TOOLTIP_LABEL_STYLE,
  GRID_PROPS,
  X_AXIS_PROPS,
  Y_AXIS_PROPS,
} from '../config'
import { SummaryCard } from './SummaryCard'

// ─── Types ───────────────────────────────────────────────────────────────────

interface PurchaseTimingChartProps {
  data: PurchaseTimingResponse
}

// ─── Colors ──────────────────────────────────────────────────────────────────

const BUCKET_COLORS = [
  '#059669', // 0-30: emerald-600 (best)
  '#10B981', // 31-60: emerald-500
  '#34D399', // 61-90: emerald-400
  '#6EE7B7', // 91-120: emerald-300
  '#A7F3D0', // 121-180: emerald-200
  '#D1FAE5', // 180+: emerald-100 (slowest)
]

// ─── Component ───────────────────────────────────────────────────────────────

export const PurchaseTimingChart = memo(function PurchaseTimingChart({
  data
}: PurchaseTimingChartProps) {
  const { t } = useTranslation()

  const chartData = useMemo(() => {
    return data.buckets.map((bucket, index) => ({
      bucket: bucket.bucket + ' ' + t('retention.days'),
      customers: bucket.customers,
      percentage: bucket.percentage,
      avgDays: bucket.avgDays,
      color: BUCKET_COLORS[index] || BUCKET_COLORS[BUCKET_COLORS.length - 1]
    }))
  }, [data.buckets, t])

  const insightText = useMemo(() => {
    if (data.buckets[0]?.percentage > 30) {
      return t('retention.insightFast', {
        pct: formatPercent(data.buckets[0].percentage),
        defaultValue: `${formatPercent(data.buckets[0].percentage)} of repeat customers return within 30 days - great for consumable products!`
      })
    }
    if (data.summary.medianDays && data.summary.medianDays > 90) {
      return t('retention.insightText', { days: Math.round(data.summary.medianDays) })
    }
    return t('retention.timingTrack')
  }, [data, t])

  return (
    <div>
      {/* Summary Stats */}
      <div className="grid grid-cols-3 gap-4 mb-6">
        <SummaryCard
          label={t('retention.repeatCustomers')}
          value={formatNumber(data.summary.totalRepeatCustomers)}
          variant="emerald"
          icon={<Users size={28} />}
        />
        <SummaryCard
          label={t('retention.medianDays')}
          value={data.summary.medianDays != null ? String(data.summary.medianDays) : '-'}
          subtitle={t('retention.to2ndPurchase')}
          icon={<Clock size={28} />}
        />
        <SummaryCard
          label={t('retention.averageDays')}
          value={data.summary.avgDays ? String(Math.round(data.summary.avgDays)) : '-'}
          subtitle={t('retention.to2ndPurchase')}
          icon={<TrendingUp size={28} />}
        />
      </div>

      {/* Chart */}
      <div style={{ height: CHART_DIMENSIONS.height.lg }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={chartData} margin={CHART_DIMENSIONS.margin.default}>
            <CartesianGrid {...GRID_PROPS} />
            <XAxis
              dataKey="bucket"
              {...X_AXIS_PROPS}
              tick={{ fontSize: 11 }}
            />
            <YAxis
              {...Y_AXIS_PROPS}
              tickFormatter={(v) => formatNumber(v)}
            />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              labelStyle={TOOLTIP_LABEL_STYLE}
              formatter={(value) => {
                if (typeof value === 'number') {
                  return [formatNumber(value), t('retention.customers')]
                }
                return [String(value), t('retention.customers')]
              }}
              labelFormatter={(label) => `${t('retention.timeTo2ndPurchase')} ${label}`}
            />
            <Bar
              dataKey="customers"
              name="customers"
              radius={[4, 4, 0, 0]}
            >
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Insights */}
      <div className="mt-4 p-3 bg-blue-50 border border-blue-200 rounded-lg">
        <p className="text-sm text-blue-800">
          <strong>{t('retention.insight')}</strong> {insightText}
        </p>
      </div>
    </div>
  )
})
