import { useMemo, useState, useCallback, memo } from 'react'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
  LabelList,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import {
  CHART_THEME,
  TOOLTIP_STYLE,
  GRID_PROPS,
  Y_AXIS_PROPS,
  BAR_PROPS,
  truncateText,
} from './config'
import { useProductPerformance, useCategoryBreakdown } from '../../hooks'
import { formatCurrency, formatNumber } from '../../utils/formatters'
import { CATEGORY_COLORS } from '../../utils/colors'
import { Button } from '../ui'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ChartDataPoint {
  name: string
  fullName: string
  value: number
  color: string
  percent: number
  label: string
}

// ─── Label Formatter ──────────────────────────────────────────────────────────

const formatShortCurrency = (value: number): string => {
  if (value >= 1000000) {
    return `₴${(value / 1000000).toFixed(1)}M`
  }
  if (value >= 1000) {
    return `₴${(value / 1000).toFixed(0)}K`
  }
  return `₴${value}`
}

// ─── Custom Tooltip ──────────────────────────────────────────────────────────

interface TooltipProps {
  active?: boolean
  payload?: Array<{
    payload: ChartDataPoint
  }>
  isSubcategory?: boolean
}

function CustomTooltip({ active, payload, isSubcategory }: TooltipProps) {
  if (!active || !payload?.length) return null

  const data = payload[0]?.payload
  if (!data) return null

  return (
    <div
      style={{
        ...TOOLTIP_STYLE,
        padding: '12px 16px',
        minWidth: '180px',
      }}
    >
      {/* Category name with color indicator */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        marginBottom: '8px',
        paddingBottom: '8px',
        borderBottom: `1px solid ${CHART_THEME.border}`,
      }}>
        <div style={{
          width: '12px',
          height: '12px',
          borderRadius: '3px',
          background: data.color,
          flexShrink: 0,
        }} />
        <span style={{
          fontWeight: 600,
          color: CHART_THEME.text,
          fontSize: '13px',
        }}>
          {data.fullName}
        </span>
      </div>

      {/* Revenue */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '4px',
      }}>
        <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>Revenue</span>
        <span style={{ fontWeight: 600, color: CHART_THEME.text, fontSize: '13px' }}>
          {formatCurrency(data.value)}
        </span>
      </div>

      {/* Percentage */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}>
        <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>Share</span>
        <span style={{
          fontWeight: 600,
          color: data.color,
          fontSize: '13px',
        }}>
          {data.percent.toFixed(1)}%
        </span>
      </div>

      {/* Drill-down hint for parent categories */}
      {!isSubcategory && (
        <div style={{
          marginTop: '8px',
          paddingTop: '8px',
          borderTop: `1px solid ${CHART_THEME.border}`,
          textAlign: 'center',
        }}>
          <span style={{
            color: CHART_THEME.muted,
            fontSize: '11px',
            fontStyle: 'italic',
          }}>
            Click to see subcategories
          </span>
        </div>
      )}
    </div>
  )
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

  const { chartData, totalRevenue } = useMemo(() => {
    let items: { name: string; value: number }[] = []

    // Drilled-down view (subcategories)
    if (selectedCategory && breakdownData?.labels?.length) {
      items = breakdownData.labels.map((label: string, index: number) => ({
        name: label || 'Unknown',
        value: breakdownData.revenue?.[index] ?? 0,
      }))
    }
    // Top-level view (parent categories)
    else if (performanceData?.categoryBreakdown?.labels?.length) {
      items = performanceData.categoryBreakdown.labels.map((label, index) => ({
        name: label || 'Unknown',
        value: performanceData.categoryBreakdown.revenue?.[index] ?? 0,
      }))
    }

    // Sort by value descending
    items.sort((a, b) => b.value - a.value)

    // Calculate total
    const total = items.reduce((sum, item) => sum + item.value, 0)

    // Build chart data with all fields
    const processed: ChartDataPoint[] = items.map((item, index) => {
      const percent = total > 0 ? (item.value / total) * 100 : 0
      return {
        name: truncateText(item.name, 18),
        fullName: item.name,
        value: item.value,
        color: CATEGORY_COLORS[index % CATEGORY_COLORS.length],
        percent,
        label: `${formatShortCurrency(item.value)} (${percent.toFixed(0)}%)`,
      }
    })

    return { chartData: processed, totalRevenue: total }
  }, [performanceData, breakdownData, selectedCategory])

  const handleBarClick = useCallback((data: ChartDataPoint) => {
    if (!selectedCategory) {
      setSelectedCategory(data.fullName)
    }
  }, [selectedCategory])

  const handleBack = useCallback(() => {
    setSelectedCategory(null)
  }, [])

  const title = selectedCategory
    ? `${selectedCategory}`
    : 'Sales by Category'

  const isEmpty = !isLoading && chartData.length === 0

  // Dynamic height based on number of items
  const chartHeight = Math.max(200, chartData.length * 40 + 40)

  return (
    <ChartContainer
      title={title}
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetchPerformance}
      isEmpty={isEmpty}
      height="auto"
      ariaLabel={`Bar chart showing ${selectedCategory ? 'subcategory' : 'category'} sales breakdown`}
      action={
        selectedCategory && (
          <Button size="sm" variant="ghost" onClick={handleBack}>
            ← Back to Categories
          </Button>
        )
      }
    >
      {/* Category count summary */}
      <div className="flex items-center gap-4 mb-4 text-sm">
        <div className="flex items-center gap-2">
          <span className="text-slate-500">
            {selectedCategory ? 'Subcategories:' : 'Categories:'}
          </span>
          <span className="font-semibold text-slate-700">{formatNumber(chartData.length)}</span>
        </div>
        <div className="flex items-center gap-2">
          <span className="text-slate-500">Total:</span>
          <span className="font-semibold text-green-600">{formatCurrency(totalRevenue)}</span>
        </div>
      </div>

      {/* Bar Chart */}
      <div style={{ height: chartHeight }}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            layout="vertical"
            margin={{ left: 10, right: 80, top: 5, bottom: 5 }}
          >
            <CartesianGrid {...GRID_PROPS} horizontal={false} />
            <XAxis
              type="number"
              hide={true}
            />
            <YAxis
              type="category"
              dataKey="name"
              {...Y_AXIS_PROPS}
              width={100}
              tick={{ fontSize: 11 }}
            />
            <Tooltip
              content={<CustomTooltip isSubcategory={!!selectedCategory} />}
              cursor={{ fill: 'rgba(0, 0, 0, 0.04)' }}
            />
            <Bar
              dataKey="value"
              {...BAR_PROPS}
              onClick={(data) => handleBarClick(data as unknown as ChartDataPoint)}
              style={{ cursor: selectedCategory ? 'default' : 'pointer' }}
            >
              {chartData.map((entry, index) => (
                <Cell key={`cell-${index}`} fill={entry.color} />
              ))}
              <LabelList
                dataKey="label"
                position="right"
                style={{
                  fill: CHART_THEME.text,
                  fontSize: 10,
                  fontWeight: 500,
                }}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>

      {/* Hint for drill-down */}
      {!selectedCategory && chartData.length > 0 && (
        <p className="text-xs text-slate-400 text-center mt-3">
          Click a category to view subcategories
        </p>
      )}
    </ChartContainer>
  )
})
