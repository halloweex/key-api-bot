import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Legend,
  LabelList,
  Cell,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import { CHART_THEME, GRID_PROPS, X_AXIS_PROPS, Y_AXIS_PROPS } from './chartConfig'
import { useRevenueTrend } from '../hooks'
import { useFilterStore } from '../store/filterStore'
import { getComparisonLabel, getPeriodLabels } from './revenueTrendHelpers'
import { PREV_MONTH_BAR_COLOR, type CompareType } from './revenueTrendTypes'
import { useChartData } from './useRevenueTrendChartData'
import { RevenueTrendTooltip } from './RevenueTrendTooltip'
import { RevenueTrendLegend } from './RevenueTrendLegend'
import { RevenueTrendGradients } from './RevenueTrendGradients'
import { RevenueTrendActions } from './RevenueTrendActions'
import { RevenueTrendInfo } from './RevenueTrendInfo'

export const RevenueTrendChart = memo(function RevenueTrendChart() {
  const { t } = useTranslation()
  const [compareType, setCompareType] = useState<CompareType>('year_ago')
  const { data, isLoading, error, refetch } = useRevenueTrend(compareType)
  const { period } = useFilterStore()

  const allPeriodLabels = getPeriodLabels(t)
  const basePeriodLabels = allPeriodLabels[period] || allPeriodLabels.custom

  const periodLabels = useMemo(
    () => ({
      current: basePeriodLabels.current,
      previous: getComparisonLabel(compareType, basePeriodLabels.previous, t),
    }),
    [basePeriodLabels, compareType, t],
  )

  const growthData = data?.comparison?.totals
  const forecast = data?.forecast

  const { chartData, hasComparison, hasPrevMonthDays, hasForecast } = useChartData(data, period)

  const isEmpty = !isLoading && chartData.length === 0

  return (
    <ChartContainer
      title={t('chart.revenueTrend')}
      titleExtra={<RevenueTrendInfo title={t('chart.revenueTrend')} hasForecast={hasForecast} t={t} />}
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="xl"
      ariaLabel={t('chart.revenueTrend')}
      action={
        <RevenueTrendActions
          forecast={forecast}
          growthData={growthData}
          isLoading={isLoading}
          compareType={compareType}
          onCompareTypeChange={setCompareType}
          t={t}
        />
      }
    >
      <div className="flex flex-col h-[350px] sm:h-[400px] lg:h-[450px]">
        <div style={{ flex: 1, minHeight: 0 }}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart
              data={chartData}
              margin={{ top: 25, right: 15, left: 5, bottom: 10 }}
              barGap={0}
              barCategoryGap="8%"
            >
              <RevenueTrendGradients />
              <CartesianGrid {...GRID_PROPS} vertical={false} />

              <XAxis
                dataKey="shortDate"
                {...X_AXIS_PROPS}
                interval={chartData.length > 14 ? Math.floor(chartData.length / 7) : 0}
                angle={chartData.length > 20 ? -45 : 0}
                textAnchor={chartData.length > 20 ? 'end' : 'middle'}
                height={chartData.length > 20 ? 50 : 30}
              />

              <YAxis
                {...Y_AXIS_PROPS}
                hide={true}
                width={0}
                domain={[0, (dataMax: number) => Math.ceil((dataMax * 1.15) / 1000) * 1000]}
              />

              <Tooltip
                content={<RevenueTrendTooltip periodLabels={periodLabels} t={t} />}
                cursor={{ fill: 'rgba(0, 0, 0, 0.04)' }}
              />

              <Bar
                dataKey="revenue"
                name={periodLabels.current}
                stackId="revenue"
                fill={CHART_THEME.primary}
                radius={[4, 4, 0, 0]}
                maxBarSize={50}
              >
                {chartData.map((entry, index) => (
                  <Cell
                    key={`cell-${index}`}
                    fill={entry.isCurrentMonth ? CHART_THEME.primary : PREV_MONTH_BAR_COLOR}
                  />
                ))}
                <LabelList
                  dataKey="peakLabel"
                  position="top"
                  style={{
                    fill: CHART_THEME.primary,
                    fontSize: 11,
                    fontWeight: 600,
                  }}
                />
              </Bar>

              {hasForecast && (
                <Bar
                  dataKey="forecastRevenue"
                  name={t('chart.predicted')}
                  stackId="revenue"
                  fill="url(#forecastBarGradient)"
                  radius={[4, 4, 0, 0]}
                  maxBarSize={50}
                />
              )}

              {hasComparison && (
                <Line
                  type="monotone"
                  dataKey="prevRevenue"
                  name={`${periodLabels.previous} ${t('chart.trendSuffix')}`}
                  stroke={CHART_THEME.muted}
                  strokeWidth={2}
                  strokeDasharray="5 5"
                  dot={false}
                  activeDot={{ r: 4, fill: CHART_THEME.muted, stroke: '#fff', strokeWidth: 2 }}
                />
              )}

              <Legend content={() => null} />
            </ComposedChart>
          </ResponsiveContainer>
        </div>

        <RevenueTrendLegend
          periodLabels={periodLabels}
          hasComparison={hasComparison}
          hasPrevMonthDays={hasPrevMonthDays}
          hasForecast={hasForecast}
          t={t}
        />
      </div>
    </ChartContainer>
  )
})
