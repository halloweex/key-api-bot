import { useMemo, useState, useCallback, memo } from 'react'
import {
  PieChart,
  Pie,
  Cell,
  Tooltip,
  ResponsiveContainer,
  Legend,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import {
  CHART_THEME,
  CHART_DIMENSIONS,
  TOOLTIP_STYLE,
  LEGEND_PROPS,
  PIE_PROPS,
  formatPieLabel,
} from './config'
import { useProductPerformance, useCategoryBreakdown } from '../../hooks'
import { formatCurrency } from '../../utils/formatters'
import { CATEGORY_COLORS } from '../../utils/colors'
import { Button } from '../ui'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  name: string
  value: number
  color: string
  [key: string]: string | number  // Recharts compatibility
}

// ─── Component ───────────────────────────────────────────────────────────────

export const CategoryChart = memo(function CategoryChart() {
  const [selectedCategory, setSelectedCategory] = useState<string | null>(null)

  const {
    data: performanceData,
    isLoading: loadingPerformance,
    error: performanceError,
    refetch: refetchPerformance,
  } = useProductPerformance()

  const {
    data: breakdownData,
    isLoading: loadingBreakdown,
  } = useCategoryBreakdown(selectedCategory)

  const isLoading = selectedCategory ? loadingBreakdown : loadingPerformance
  const error = performanceError

  const chartData = useMemo<ChartDataPoint[]>(() => {
    // Drilled-down view
    if (selectedCategory && breakdownData?.labels?.length) {
      return breakdownData.labels.map((label: string, index: number) => ({
        name: label || 'Unknown',
        value: breakdownData.revenue?.[index] ?? 0,
        color: CATEGORY_COLORS[index % CATEGORY_COLORS.length],
      }))
    }

    // Top-level view
    if (!performanceData?.categoryBreakdown?.labels?.length) return []

    return performanceData.categoryBreakdown.labels.map((label, index) => ({
      name: label || 'Unknown',
      value: performanceData.categoryBreakdown.revenue?.[index] ?? 0,
      color: CATEGORY_COLORS[index % CATEGORY_COLORS.length],
    }))
  }, [performanceData, breakdownData, selectedCategory])

  const totalRevenue = useMemo(
    () => chartData.reduce((sum, item) => sum + item.value, 0),
    [chartData]
  )

  const handleClick = useCallback((data: { name: string }) => {
    setSelectedCategory(prev => prev ? null : data.name)
  }, [])

  const handleBack = useCallback(() => {
    setSelectedCategory(null)
  }, [])

  const title = selectedCategory
    ? `${selectedCategory} - Subcategories`
    : 'Sales by Category'

  const isEmpty = !isLoading && chartData.length === 0

  return (
    <ChartContainer
      title={title}
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetchPerformance}
      isEmpty={isEmpty}
      height="lg"
      ariaLabel={`Pie chart showing ${selectedCategory ? 'subcategory' : 'category'} sales breakdown`}
      action={
        selectedCategory && (
          <Button size="sm" variant="ghost" onClick={handleBack}>
            &larr; Back
          </Button>
        )
      }
    >
      <div style={{ height: CHART_DIMENSIONS.height.lg }}>
        <ResponsiveContainer width="100%" height="100%">
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
              label={({ percent }) => formatPieLabel(percent ?? 0)}
              {...PIE_PROPS}
            >
              {chartData.map((entry, index) => (
                <Cell
                  key={`cell-${index}`}
                  fill={entry.color}
                  stroke={CHART_THEME.background}
                  strokeWidth={2}
                />
              ))}
            </Pie>
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              formatter={(value, name) => [formatCurrency(Number(value) || 0), String(name)]}
            />
            <Legend
              {...LEGEND_PROPS}
              wrapperStyle={{ ...LEGEND_PROPS.wrapperStyle, fontSize: '12px' }}
              layout="vertical"
              align="right"
              verticalAlign="middle"
            />
          </PieChart>
        </ResponsiveContainer>
      </div>

      {/* Hint */}
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
})
