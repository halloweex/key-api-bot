import { memo, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import {
  ComposedChart,
  Bar,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ReferenceLine,
  ResponsiveContainer,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import { GRID_PROPS, LINE_PROPS, TOOLTIP_STYLE, X_AXIS_PROPS, Y_AXIS_PROPS } from './chartConfig'
import type { TgBotActivityDay } from '../types/api'

// People who did anything that Kyiv day, and the registrations among them.
// Two scales of one unit (people), so one axis.

const ACTIVE_COLOR = '#2563EB'      // blue-600
const REGISTERED_COLOR = '#10B981'  // emerald-500

interface TgBotActivityChartProps {
  daily: TgBotActivityDay[]
  /** First day the bot's log has. Marked on the axis when it falls inside the window. */
  logStarts: string | null
  isLoading?: boolean
  error?: Error | null
  onRetry?: () => void
}

const short = (iso: string) => {
  const [, m, d] = iso.split('-')
  return d && m ? `${d}.${m}` : iso
}

export const TgBotActivityChart = memo(function TgBotActivityChart({
  daily, logStarts, isLoading, error, onRetry,
}: TgBotActivityChartProps) {
  const { t } = useTranslation()
  const data = useMemo(() => daily.map((d) => ({ ...d, label: short(d.day) })), [daily])
  const marker = logStarts && daily.some((d) => d.day === logStarts) ? short(logStarts) : null

  return (
    <ChartContainer
      title={t('tgBot.dailyTitle')}
      isLoading={isLoading}
      error={error ?? null}
      onRetry={onRetry}
      isEmpty={!isLoading && data.length === 0}
      height="lg"
      ariaLabel={t('tgBot.dailyTitle')}
    >
      <div className="h-[300px] sm:h-[340px]">
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 20, right: 10, left: -10, bottom: 5 }}>
            <CartesianGrid {...GRID_PROPS} vertical={false} />
            <XAxis
              dataKey="label"
              {...X_AXIS_PROPS}
              interval={data.length > 14 ? Math.floor(data.length / 8) : 0}
            />
            <YAxis {...Y_AXIS_PROPS} allowDecimals={false} width={40} />
            <Tooltip
              contentStyle={TOOLTIP_STYLE}
              labelFormatter={(_, payload) => payload?.[0]?.payload?.day ?? ''}
            />
            <Legend />
            {marker && (
              <ReferenceLine
                x={marker}
                stroke="#94A3B8"
                strokeDasharray="4 4"
                label={{ value: t('tgBot.logStarts'), position: 'insideTopLeft', fontSize: 11, fill: '#64748B' }}
              />
            )}
            <Bar
              dataKey="registered"
              name={t('tgBot.registrations')}
              fill={REGISTERED_COLOR}
              radius={[3, 3, 0, 0]}
              maxBarSize={18}
            />
            <Line
              dataKey="active"
              name={t('tgBot.active')}
              stroke={ACTIVE_COLOR}
              type="monotone"
              {...LINE_PROPS}
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </ChartContainer>
  )
})

export default TgBotActivityChart
