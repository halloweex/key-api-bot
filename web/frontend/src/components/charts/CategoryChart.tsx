import { useMemo, useState, useCallback } from 'react'
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import { useProductPerformance, useCategoryBreakdown } from '../../hooks'
import { formatCurrency } from '../../utils/formatters'
import { CATEGORY_COLORS } from '../../utils/colors'
import { Button } from '../ui'

export function CategoryChart() {
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null)

  const { data: performanceData, isLoading: loadingPerformance, error: performanceError } = useProductPerformance()
  const { data: breakdownData, isLoading: loadingBreakdown } = useCategoryBreakdown(selectedCategory)

  const isLoading = selectedCategory ? loadingBreakdown : loadingPerformance
  const error = performanceError

  // Use breakdown data if drilled down, otherwise use category breakdown from performance
  const chartData = useMemo(() => {
    if (selectedCategory && breakdownData?.labels?.length) {
      return breakdownData.labels.map((label: string, index: number) => ({
        name: label || 'Unknown',
        value: breakdownData.revenue?.[index] ?? 0,
        color: CATEGORY_COLORS[index % CATEGORY_COLORS.length],
      }))
    }

    if (!performanceData?.categoryBreakdown?.labels?.length) return []

    return performanceData.categoryBreakdown.labels.map((label, index) => ({
      name: label || 'Unknown',
      value: performanceData.categoryBreakdown.revenue?.[index] ?? 0,
      color: CATEGORY_COLORS[index % CATEGORY_COLORS.length],
    }))
  }, [performanceData, breakdownData, selectedCategory])

  const totalRevenue = useMemo(() =>
    chartData.reduce((sum, item) => sum + item.value, 0),
    [chartData]
  )

  const handleClick = useCallback((data: { name: string }) => {
    if (selectedCategory) {
      setSelectedCategory(null)
    } else {
      setSelectedCategory(data.name)
    }
  }, [selectedCategory])

  const handleBack = useCallback(() => {
    setSelectedCategory(null)
  }, [])

  const title = selectedCategory
    ? `${selectedCategory} - Subcategories`
    : 'Sales by Category'

  if (!isLoading && chartData.length === 0) {
    return (
      <ChartContainer title={title} isLoading={false} error={null}>
        <div className="h-72 flex items-center justify-center text-slate-500">
          No data available
        </div>
      </ChartContainer>
    )
  }

  return (
    <ChartContainer
      title={title}
      isLoading={isLoading}
      error={error as Error | null}
      action={
        selectedCategory && (
          <Button size="sm" variant="ghost" onClick={handleBack}>
            ← Back
          </Button>
        )
      }
    >
      <div className="h-72 min-h-[288px]">
        <ResponsiveContainer width="100%" height="100%" minHeight={288}>
          <PieChart>
            <Pie
              data={chartData}
              cx="50%"
              cy="50%"
              innerRadius={60}
              outerRadius={100}
              dataKey="value"
              nameKey="name"
              onClick={handleClick}
              style={{ cursor: 'pointer' }}
              label={({ percent }) =>
                (percent ?? 0) > 0.05 ? `${((percent ?? 0) * 100).toFixed(0)}%` : ''
              }
              labelLine={false}
            >
              {chartData.map((entry, index) => (
                <Cell
                  key={`cell-${index}`}
                  fill={entry.color}
                  stroke="#1E293B"
                  strokeWidth={2}
                />
              ))}
            </Pie>
            <Tooltip
              contentStyle={{
                backgroundColor: '#1E293B',
                border: '1px solid #374151',
                borderRadius: '8px',
              }}
              formatter={(value, name) => [formatCurrency(Number(value) || 0), name]}
            />
            <Legend
              wrapperStyle={{ color: '#9CA3AF', fontSize: '12px' }}
              layout="vertical"
              align="right"
              verticalAlign="middle"
            />
          </PieChart>
        </ResponsiveContainer>
      </div>

      {/* Hint text */}
      <div className="mt-2 text-center">
        <p className="text-xs text-slate-500">
          {selectedCategory
            ? 'Click chart to go back to categories'
            : 'Click a category to see subcategories'}
        </p>
      </div>

      {/* Total */}
      <div className="mt-2 pt-3 border-t border-slate-700">
        <div className="flex justify-between items-center">
          <span className="text-slate-400 text-sm">Total Revenue</span>
          <span className="text-white font-semibold">{formatCurrency(totalRevenue)}</span>
        </div>
      </div>
    </ChartContainer>
  )
}
