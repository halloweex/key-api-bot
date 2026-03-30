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
import { useMarginByBrand } from '../../hooks'
import { formatCurrency, formatPercent } from '../../utils/formatters'
import { COLORS } from '../../utils/colors'

// ─── Types ───────────────────────────────────────────────────────────────────

interface BrandDataPoint {
  name: string
  fullName: string
  profit: number
  profitLabel: string
  margin_pct: number | null
}

// ─── Formatters ──────────────────────────────────────────────────────────────

const formatShortCurrency = (value: number): string => {
  if (value >= 1000000) return `₴${(value / 1000000).toFixed(1)}M`
  if (value >= 1000) return `₴${(value / 1000).toFixed(0)}K`
  return `₴${value}`
}

// ─── Component ───────────────────────────────────────────────────────────────

export const MarginByBrandChart = memo(function MarginByBrandChart() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useMarginByBrand()

  const chartData = useMemo<BrandDataPoint[]>(() => {
    if (!data?.length) return []
    return data
      .filter((b) => b.profit !== 0)
      .slice(0, 15)
      .map((item) => ({
        name: truncateText(item.brand, 18),
        fullName: item.brand,
        profit: item.profit,
        profitLabel: formatShortCurrency(item.profit),
        margin_pct: item.margin_pct,
      }))
  }, [data])

  const isEmpty = !isLoading && chartData.length === 0

  return (
    <ChartContainer
      title={t('margin.byBrand')}
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xxl"
      ariaLabel={t('margin.byBrandDesc')}
    >
      <div style={HEIGHT_STYLE.xxl}>
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
                const item = (props as { payload: BrandDataPoint }).payload
                return [
                  `${formatCurrency(v)} (${item.margin_pct != null ? formatPercent(item.margin_pct) : 'N/A'})`,
                  t('margin.grossProfit'),
                ]
              }}
              labelFormatter={(_label, payload) => {
                const item = payload?.[0]?.payload as BrandDataPoint | undefined
                return item?.fullName || String(_label)
              }}
            />
            <Bar
              dataKey="profit"
              fill={COLORS.success}
              {...BAR_PROPS}
            >
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
