import { useMemo } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  LabelList,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import { useTopProducts } from '../../hooks'
import { formatNumber } from '../../utils/formatters'
import { COLORS } from '../../utils/colors'

// Truncate long product names
function truncateName(name: string, maxLength = 25): string {
  if (name.length <= maxLength) return name
  return name.slice(0, maxLength - 3) + '...'
}

export function TopProductsChart() {
  const { data, isLoading, error } = useTopProducts()

  const chartData = useMemo(() => {
    if (!data) return []
    return data.labels.map((label, index) => ({
      name: truncateName(label),
      fullName: label,
      quantity: data.data[index],
      percentage: data.percentages[index],
    }))
  }, [data])

  return (
    <ChartContainer
      title="Top 10 Products"
      isLoading={isLoading}
      error={error as Error | null}
    >
      <div className="h-80">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            layout="vertical"
            margin={{ left: 10, right: 60, top: 5, bottom: 5 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#374151" horizontal={false} />
            <XAxis
              type="number"
              stroke="#9CA3AF"
              fontSize={12}
              tickLine={false}
            />
            <YAxis
              type="category"
              dataKey="name"
              stroke="#9CA3AF"
              fontSize={11}
              tickLine={false}
              width={150}
            />
            <Tooltip
              contentStyle={{
                backgroundColor: '#1E293B',
                border: '1px solid #374151',
                borderRadius: '8px',
              }}
              labelStyle={{ color: '#F3F4F6' }}
              formatter={(value, _name, props) => {
                const numValue = Number(value) || 0
                const percentage = (props.payload as { percentage?: number })?.percentage ?? 0
                return [
                  `${formatNumber(numValue)} (${percentage.toFixed(1)}%)`,
                  'Quantity',
                ]
              }}
              labelFormatter={(_label, payload) => {
                const item = payload?.[0]?.payload as { fullName?: string } | undefined
                return item?.fullName || String(_label)
              }}
            />
            <Bar
              dataKey="quantity"
              fill={COLORS.primary}
              radius={[0, 4, 4, 0]}
            >
              <LabelList
                dataKey="percentage"
                position="right"
                fill="#9CA3AF"
                fontSize={11}
                formatter={(value) => `${Number(value).toFixed(1)}%`}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </ChartContainer>
  )
}
