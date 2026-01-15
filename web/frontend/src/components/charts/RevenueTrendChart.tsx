import { useMemo } from 'react'
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
import { ChartContainer } from './ChartContainer'
import { useRevenueTrend } from '../../hooks'
import { formatCurrency } from '../../utils/formatters'
import { COLORS } from '../../utils/colors'

export function RevenueTrendChart() {
  const { data, isLoading, error } = useRevenueTrend()

  const chartData = useMemo(() => {
    if (!data?.labels?.length) return []
    return data.labels.map((label, index) => ({
      date: label,
      revenue: data.revenue?.[index] ?? 0,
      orders: data.orders?.[index] ?? 0,
    }))
  }, [data])

  if (!isLoading && chartData.length === 0) {
    return (
      <ChartContainer title="Revenue Trend" isLoading={false} error={null}>
        <div className="h-72 flex items-center justify-center text-slate-500">
          No data available
        </div>
      </ChartContainer>
    )
  }

  return (
    <ChartContainer
      title="Revenue Trend"
      isLoading={isLoading}
      error={error as Error | null}
    >
      <div className="h-72 min-h-[288px]">
        <ResponsiveContainer width="100%" height="100%" minHeight={288}>
          <LineChart data={chartData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
            <XAxis
              dataKey="date"
              stroke="#9CA3AF"
              fontSize={12}
              tickLine={false}
            />
            <YAxis
              yAxisId="revenue"
              stroke="#9CA3AF"
              fontSize={12}
              tickLine={false}
              tickFormatter={(value) => `${(value / 1000).toFixed(0)}k`}
            />
            <YAxis
              yAxisId="orders"
              orientation="right"
              stroke="#9CA3AF"
              fontSize={12}
              tickLine={false}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: '#1E293B',
                border: '1px solid #374151',
                borderRadius: '8px',
              }}
              labelStyle={{ color: '#F3F4F6' }}
              formatter={(value, name) => {
                const numValue = Number(value) || 0
                return [
                  name === 'revenue' ? formatCurrency(numValue) : numValue,
                  name === 'revenue' ? 'Revenue' : 'Orders',
                ]
              }}
            />
            <Legend
              wrapperStyle={{ color: '#9CA3AF' }}
            />
            <Line
              yAxisId="revenue"
              type="monotone"
              dataKey="revenue"
              name="Revenue"
              stroke={COLORS.primary}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: COLORS.primary }}
            />
            <Line
              yAxisId="orders"
              type="monotone"
              dataKey="orders"
              name="Orders"
              stroke={COLORS.accent}
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 4, fill: COLORS.accent }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </ChartContainer>
  )
}
