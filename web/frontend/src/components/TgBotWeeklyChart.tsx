import { memo, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import { GRID_PROPS, TOOLTIP_STYLE, X_AXIS_PROPS, Y_AXIS_PROPS } from './chartConfig'
import type { TgBotActivityWeek } from '../types/api'

// Who was in the bot each Kyiv week: first seen that week, or back again.
// The two add up to the week's active people, so they stack.

const NEW_COLOR = '#8B5CF6'        // violet-500
const RETURNING_COLOR = '#2563EB'  // blue-600

interface TgBotWeeklyChartProps {
  weekly: TgBotActivityWeek[]
  isLoading?: boolean
  error?: Error | null
  onRetry?: () => void
}

export const TgBotWeeklyChart = memo(function TgBotWeeklyChart({
  weekly, isLoading, error, onRetry,
}: TgBotWeeklyChartProps) {
  const { t } = useTranslation()
  const data = useMemo(() => weekly.map((w) => {
    const [, m, d] = w.week.split('-')
    return { ...w, label: d && m ? `${d}.${m}` : w.week }
  }), [weekly])

  return (
    <ChartContainer
      title={t('tgBot.weeklyTitle')}
      isLoading={isLoading}
      error={error ?? null}
      onRetry={onRetry}
      isEmpty={!isLoading && data.length === 0}
      height="lg"
      ariaLabel={t('tgBot.weeklyTitle')}
    >
      <div className="h-[300px] sm:h-[340px]">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={data} margin={{ top: 20, right: 10, left: -10, bottom: 5 }}>
            <CartesianGrid {...GRID_PROPS} vertical={false} />
            <XAxis dataKey="label" {...X_AXIS_PROPS} />
            <YAxis {...Y_AXIS_PROPS} allowDecimals={false} width={40} />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              labelFormatter={(_, payload) =>
                payload?.[0]?.payload ? t('tgBot.weekOf', { date: payload[0].payload.week }) : ''}
            />
            <Legend />
            <Bar dataKey="returning" name={t('tgBot.returning')} stackId="w" fill={RETURNING_COLOR} maxBarSize={40} />
            <Bar dataKey="new" name={t('tgBot.new')} stackId="w" fill={NEW_COLOR} radius={[3, 3, 0, 0]} maxBarSize={40} />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </ChartContainer>
  )
})

export default TgBotWeeklyChart
