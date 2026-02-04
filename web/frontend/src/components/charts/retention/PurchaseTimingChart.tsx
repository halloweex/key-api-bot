import { memo, useMemo } from 'react'
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
  const chartData = useMemo(() => {
    return data.buckets.map((bucket, index) => ({
      bucket: bucket.bucket + ' days',
      customers: bucket.customers,
      percentage: bucket.percentage,
      avgDays: bucket.avgDays,
      color: BUCKET_COLORS[index] || BUCKET_COLORS[BUCKET_COLORS.length - 1]
    }))
  }, [data.buckets])

  return (
    <div>
      {/* Summary Stats */}
      <div className="grid grid-cols-3 gap-4 mb-6">
        <div className="bg-gradient-to-br from-emerald-50 to-emerald-100/50 border border-emerald-200 rounded-xl p-4">
          <p className="text-xs text-emerald-700 font-medium">Repeat Customers</p>
          <p className="text-xl font-bold text-emerald-800">
            {formatNumber(data.summary.totalRepeatCustomers)}
          </p>
        </div>
        <div className="bg-gradient-to-br from-slate-50 to-slate-100/50 border border-slate-200 rounded-xl p-4">
          <p className="text-xs text-slate-600 font-medium">Median Days</p>
          <p className="text-xl font-bold text-slate-800">
            {data.summary.medianDays ?? '-'}
          </p>
          <p className="text-xs text-slate-500">to 2nd purchase</p>
        </div>
        <div className="bg-gradient-to-br from-slate-50 to-slate-100/50 border border-slate-200 rounded-xl p-4">
          <p className="text-xs text-slate-600 font-medium">Average Days</p>
          <p className="text-xl font-bold text-slate-800">
            {data.summary.avgDays ? Math.round(data.summary.avgDays) : '-'}
          </p>
          <p className="text-xs text-slate-500">to 2nd purchase</p>
        </div>
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
                  return [formatNumber(value), 'Customers']
                }
                return [String(value), 'Customers']
              }}
              labelFormatter={(label) => `Time to 2nd Purchase: ${label}`}
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
          <strong>Insight:</strong> {' '}
          {data.buckets[0]?.percentage > 30
            ? `${formatPercent(data.buckets[0].percentage)} of repeat customers return within 30 days - great for consumable products!`
            : data.summary.medianDays && data.summary.medianDays > 90
            ? `Median repurchase time is ${Math.round(data.summary.medianDays)} days - consider re-engagement campaigns at 60-day mark.`
            : `Track when customers typically repurchase to optimize your email automation timing.`
          }
        </p>
      </div>
    </div>
  )
})
