import { memo } from 'react'
import {
  ResponsiveContainer, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip,
} from 'recharts'
import { useCategoryCombos } from '../../hooks/useApi'
import { formatNumber } from '../../utils/formatters'
import { InfoPopover } from '../ui/InfoPopover'

export const CategoryCombosChart = memo(function CategoryCombosChart() {
  const { data, isLoading } = useCategoryCombos()

  const chartData = data?.map((d) => ({
    name: `${d.categoryA} + ${d.categoryB}`,
    value: d.coOccurrence,
  })) ?? []

  return (
    <div className="bg-white rounded-lg border border-slate-200 shadow-sm p-4">
      <div className="flex items-center gap-1.5 mb-3">
        <h3 className="text-sm font-semibold text-slate-800">Category Combinations</h3>
        <InfoPopover>
          <p className="text-xs text-slate-300">Most common category pairs appearing in the same order.</p>
        </InfoPopover>
      </div>

      {isLoading ? (
        <div className="h-[280px] flex items-center justify-center">
          <div className="w-6 h-6 border-2 border-purple-500 border-t-transparent rounded-full animate-spin" />
        </div>
      ) : !chartData.length ? (
        <div className="h-[280px] flex items-center justify-center text-sm text-slate-400">
          No data available
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={280}>
          <BarChart data={chartData} layout="vertical" margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" horizontal={false} />
            <XAxis type="number" tick={{ fontSize: 11 }} stroke="#94a3b8" tickFormatter={(v: number) => formatNumber(v)} />
            <YAxis
              type="category"
              dataKey="name"
              tick={{ fontSize: 11 }}
              stroke="#94a3b8"
              width={180}
            />
            <Tooltip
              contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e2e8f0' }}
              formatter={(value) => [formatNumber(Number(value) || 0), 'Orders']}
            />
            <Bar dataKey="value" fill="#7C3AED" radius={[0, 4, 4, 0]} fillOpacity={0.8} />
          </BarChart>
        </ResponsiveContainer>
      )}
    </div>
  )
})
