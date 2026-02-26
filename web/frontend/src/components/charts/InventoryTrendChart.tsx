import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { InfoPopover } from '../ui/InfoPopover'
import {
  ComposedChart,
  Line,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from 'recharts'
import { ChartContainer } from './ChartContainer'
import { useInventoryTrend } from '../../hooks'
import { formatCurrency, formatNumber } from '../../utils/formatters'

// ─── Component ───────────────────────────────────────────────────────────────

function InventoryTrendChartComponent() {
  const { t } = useTranslation()
  const [days, setDays] = useState(90)
  const [granularity, setGranularity] = useState<'daily' | 'monthly'>('daily')
  const { data, isLoading, error } = useInventoryTrend(days, granularity)

  // Transform data for Recharts
  const chartData = data?.labels.map((label, index) => ({
    name: label,
    value: data.value[index],
    quantity: data.quantity[index],
    reserve: data.reserve[index],
    change: data.valueChange?.[index] || 0,
  })) || []

  return (
    <ChartContainer
      title={t('inventory.trendLabel')}
      titleExtra={
        <InfoPopover title={t('inventory.trendLabel')}>
          <div className="space-y-2">
            <p className="text-xs text-slate-300">{t('inventory.trendInfo1')}</p>
            <p className="text-xs text-slate-300">
              <strong className="text-indigo-400">{t('inventory.stockValue')}:</strong> {t('inventory.trendInfo2')}
            </p>
            <p className="text-xs text-slate-300">
              <strong className="text-emerald-400">{t('chart.quantity')}:</strong> {t('inventory.trendInfo3')}
            </p>
            <p className="text-xs text-slate-300">
              <strong className="text-slate-400">{t('inventory.trendInfo4')}</strong>
            </p>
            <p className="text-xs text-slate-300">
              <strong className="text-blue-400">{t('inventory.monthly')}:</strong> {t('inventory.trendInfo5')}
            </p>
          </div>
        </InfoPopover>
      }
      isLoading={isLoading}
      error={error}
      className="col-span-1"
      ariaLabel={t('inventory.trendDesc')}
    >
      {data && (
        <div className="space-y-4">
          {/* Controls */}
          <div className="flex flex-wrap gap-3 items-center justify-between">
            <div className="flex gap-2">
              <PeriodButton active={days === 30} onClick={() => setDays(30)}>
                {t('inventory.30d')}
              </PeriodButton>
              <PeriodButton active={days === 90} onClick={() => setDays(90)}>
                {t('inventory.90d')}
              </PeriodButton>
              <PeriodButton active={days === 180} onClick={() => setDays(180)}>
                {t('inventory.180d')}
              </PeriodButton>
              <PeriodButton active={days === 365} onClick={() => setDays(365)}>
                {t('inventory.1y')}
              </PeriodButton>
            </div>
            <div className="flex gap-2">
              <GranularityButton
                active={granularity === 'daily'}
                onClick={() => setGranularity('daily')}
              >
                {t('inventory.daily')}
              </GranularityButton>
              <GranularityButton
                active={granularity === 'monthly'}
                onClick={() => setGranularity('monthly')}
              >
                {t('inventory.monthly')}
              </GranularityButton>
            </div>
          </div>

          {/* Summary Stats */}
          {data.summary && (
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              <StatCard
                label={t('inventory.startValue')}
                value={formatCurrency(data.summary.startValue)}
                color="text-slate-600"
              />
              <StatCard
                label={t('inventory.endValue')}
                value={formatCurrency(data.summary.endValue)}
                color="text-slate-600"
              />
              <StatCard
                label={t('inventory.change')}
                value={formatCurrency(data.summary.change)}
                subValue={`${data.summary.changePercent >= 0 ? '+' : ''}${data.summary.changePercent}%`}
                color={data.summary.change >= 0 ? 'text-emerald-600' : 'text-red-600'}
              />
              <StatCard
                label={t('inventory.dataPoints')}
                value={data.dataPoints}
                subValue={`${data.granularity}`}
                color="text-blue-600"
              />
            </div>
          )}

          {/* Chart */}
          {chartData.length > 1 ? (
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <ComposedChart data={chartData} margin={{ top: 5, right: 5, left: 5, bottom: 5 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                  <XAxis
                    dataKey="name"
                    tick={{ fontSize: 11 }}
                    tickLine={false}
                    interval="preserveStartEnd"
                  />
                  <YAxis
                    yAxisId="value"
                    orientation="left"
                    tick={{ fontSize: 11 }}
                    tickFormatter={(v) => `${(v / 1000000).toFixed(1)}M`}
                    tickLine={false}
                    axisLine={false}
                  />
                  <YAxis
                    yAxisId="quantity"
                    orientation="right"
                    tick={{ fontSize: 11 }}
                    tickFormatter={(v) => formatNumber(v)}
                    tickLine={false}
                    axisLine={false}
                  />
                  <Tooltip
                    content={<CustomTooltip t={t} />}
                    cursor={{ fill: 'rgba(0, 0, 0, 0.05)' }}
                  />
                  <Legend
                    wrapperStyle={{ fontSize: '12px' }}
                    iconType="circle"
                  />
                  <Line
                    yAxisId="value"
                    type="monotone"
                    dataKey="value"
                    name={t('inventory.stockValue')}
                    stroke="#6366f1"
                    strokeWidth={2}
                    dot={false}
                    activeDot={{ r: 4 }}
                  />
                  <Bar
                    yAxisId="quantity"
                    dataKey="quantity"
                    name={t('chart.quantity')}
                    fill="#10b981"
                    opacity={0.3}
                    radius={[2, 2, 0, 0]}
                  />
                </ComposedChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <div className="h-64 flex items-center justify-center text-slate-500">
              <div className="text-center">
                <svg className="w-12 h-12 mx-auto mb-2 text-slate-300" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                </svg>
                <p>{t('inventory.notEnoughData')}</p>
                <p className="text-sm text-slate-400">
                  {t('inventory.dailySnapshots')}
                  <br />
                  {t('inventory.checkBack')}
                </p>
              </div>
            </div>
          )}
        </div>
      )}
    </ChartContainer>
  )
}

// ─── Sub Components ──────────────────────────────────────────────────────────

interface PeriodButtonProps {
  active: boolean
  onClick: () => void
  children: React.ReactNode
}

function PeriodButton({ active, onClick, children }: PeriodButtonProps) {
  return (
    <button
      onClick={onClick}
      className={`px-3 py-1 text-sm rounded-lg transition-colors ${
        active
          ? 'bg-indigo-100 text-indigo-700 font-medium'
          : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
      }`}
    >
      {children}
    </button>
  )
}

function GranularityButton({ active, onClick, children }: PeriodButtonProps) {
  return (
    <button
      onClick={onClick}
      className={`px-3 py-1 text-sm rounded-lg transition-colors ${
        active
          ? 'bg-slate-700 text-white font-medium'
          : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
      }`}
    >
      {children}
    </button>
  )
}

interface StatCardProps {
  label: string
  value: string | number
  subValue?: string
  color: string
}

function StatCard({ label, value, subValue, color }: StatCardProps) {
  return (
    <div className="bg-slate-50 rounded-lg p-3 text-center">
      <div className={`text-xl font-bold ${color}`}>{value}</div>
      <div className="text-xs text-slate-600 font-medium">{label}</div>
      {subValue && <div className="text-xs text-slate-500">{subValue}</div>}
    </div>
  )
}

interface ChartDataPoint {
  name: string
  value: number
  quantity: number
  reserve: number
  change: number
}

interface TooltipPayload {
  name: string
  value: number
  color: string
  dataKey: string
  payload: ChartDataPoint
}

interface CustomTooltipProps {
  active?: boolean
  payload?: TooltipPayload[]
  label?: string
  t: (key: string) => string
}

function CustomTooltip({ active, payload, label, t }: CustomTooltipProps) {
  if (!active || !payload || !payload.length) return null

  const data = payload[0]?.payload
  if (!data) return null

  return (
    <div className="bg-white border border-slate-200 rounded-lg shadow-lg p-3 text-sm">
      <div className="font-medium text-slate-700 mb-2">{label}</div>
      <div className="space-y-1">
        <div className="flex justify-between gap-4">
          <span className="text-slate-500">{t('inventory.stockValue')}:</span>
          <span className="font-medium text-indigo-600">{formatCurrency(data.value)}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-slate-500">{t('chart.quantity')}:</span>
          <span className="font-medium text-emerald-600">{formatNumber(data.quantity)}</span>
        </div>
        <div className="flex justify-between gap-4">
          <span className="text-slate-500">{t('inventory.reserved')}:</span>
          <span className="font-medium text-slate-600">{formatNumber(data.reserve)}</span>
        </div>
        {data.change !== 0 && (
          <div className="flex justify-between gap-4 pt-1 border-t border-slate-100">
            <span className="text-slate-500">{t('inventory.change')}:</span>
            <span className={`font-medium ${data.change >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
              {data.change >= 0 ? '+' : ''}{formatCurrency(data.change)}
            </span>
          </div>
        )}
      </div>
    </div>
  )
}

// ─── Export ──────────────────────────────────────────────────────────────────

export const InventoryTrendChart = memo(InventoryTrendChartComponent)
