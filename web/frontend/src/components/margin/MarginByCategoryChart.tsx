import { useMemo, memo } from 'react'
import { useTranslation } from 'react-i18next'
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
import { ChartContainer } from '../charts/ChartContainer'
import {
  TOOLTIP_STYLE,
  GRID_PROPS,
  Y_AXIS_PROPS,
  BAR_PROPS,
  LABEL_STYLE,
  HEIGHT_STYLE,
  CHART_DIMENSIONS,
  truncateText,
} from '../charts/config'
import { useMarginByCategory } from '../../hooks'
import { formatCurrency, formatPercent } from '../../utils/formatters'
import { CATEGORY_COLORS } from '../../utils/colors'

// ─── Types ───────────────────────────────────────────────────────────────────

interface CategoryDataPoint {
  name: string
  fullName: string
  profit: number
  profitLabel: string
  margin_pct: number | null
  rev_share_pct: number
  fill: string
}

// ─── Formatters ──────────────────────────────────────────────────────────────

const formatShortCurrency = (value: number): string => {
  if (value >= 1000000) return `₴${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `₴${(value / 1000).toFixed(0)}K`
  return `₴${value}`
}

// ─── Component ───────────────────────────────────────────────────────────────

export const MarginByCategoryChart = memo(function MarginByCategoryChart() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useMarginByCategory()

  const chartData = useMemo<CategoryDataPoint[]>(() => {
    if (!data?.length) return []
    return data.map((item, i) => ({
      name: truncateText(item.category, 20),
      fullName: item.category,
      profit: item.profit,
      profitLabel: formatShortCurrency(item.profit),
      margin_pct: item.margin_pct,
      rev_share_pct: item.rev_share_pct,
      fill: CATEGORY_COLORS[i % CATEGORY_COLORS.length],
    }))
  }, [data])

  const isEmpty = !isLoading && chartData.length === 0

  return (
    <ChartContainer
      title={t('margin.byCategory')}
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xl"
      ariaLabel={t('margin.byCategoryDesc')}
    >
      <div style={HEIGHT_STYLE.xl}>
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={chartData}
            layout="vertical"
            margin={{ left: 10, right: 60 }}
          >
            <CartesianGrid {...GRID_PROPS} horizontal={false} />
            <XAxis type="number" hide />
            <YAxis
              type="category"
              dataKey="name"
              {...Y_AXIS_PROPS}
              fontSize={CHART_DIMENSIONS.fontSize.xs}
              width={CHART_DIMENSIONS.yAxisWidth.lg}
            />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              formatter={(value, _name, props) => {
                const v = Number(value) || 0
                const item = (props as { payload: CategoryDataPoint }).payload
                return [
                  `${formatCurrency(v)} (${item.margin_pct != null ? formatPercent(item.margin_pct) : 'N/A'})`,
                  t('margin.grossProfit'),
                ]
              }}
              labelFormatter={(_label, payload) => {
                const item = payload?.[0]?.payload as CategoryDataPoint | undefined
                if (!item) return String(_label)
                return `${item.fullName} (${formatPercent(item.rev_share_pct)} ${t('margin.revShare')})`
              }}
            />
            <Bar dataKey="profit" {...BAR_PROPS}>
              <LabelList
                dataKey="profitLabel"
                position="right"
                style={LABEL_STYLE.default}
              />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </ChartContainer>
  )
})
