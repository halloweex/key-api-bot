import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import {
  ResponsiveContainer, ComposedChart, Bar, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
} from 'recharts'
import { useBasketDistribution } from '../../hooks/useApi'
import { formatCurrency, formatNumber } from '../../utils/formatters'
import { InfoPopover } from '../ui/InfoPopover'
import { SkeletonVerticalBars } from '../ui/Skeleton'

export const BasketDistributionChart = memo(function BasketDistributionChart() {
  const { t } = useTranslation()
  const { data, isLoading } = useBasketDistribution()

  return (
    <div className="bg-white rounded-lg border border-slate-200 shadow-sm p-4">
      <div className="flex items-center gap-1.5 mb-3">
        <h3 className="text-sm font-semibold text-slate-800">{t('products.basketDistTitle')}</h3>
        <InfoPopover>
          <p className="text-xs text-slate-300">{t('products.basketDistDesc')}</p>
        </InfoPopover>
      </div>

      {isLoading ? (
        <SkeletonVerticalBars />
      ) : !data?.length ? (
        <div className="h-[280px] flex items-center justify-center text-sm text-slate-400">
          {t('chart.noData')}
        </div>
      ) : (
        <ResponsiveContainer width="100%" height={280}>
          <ComposedChart data={data} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
            <XAxis dataKey="bucket" tick={{ fontSize: 12 }} stroke="#94a3b8" />
            <YAxis
              yAxisId="left"
              tick={{ fontSize: 12 }}
              stroke="#94a3b8"
              tickFormatter={(v: number) => formatNumber(v)}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              tick={{ fontSize: 12 }}
              stroke="#94a3b8"
              tickFormatter={(v: number) => `₴${(v / 1000).toFixed(0)}k`}
            />
            <Tooltip
              contentStyle={{ fontSize: 12, borderRadius: 8, border: '1px solid #e2e8f0' }}
              formatter={(value, name) => {
                const v = Number(value) || 0
                if (name === t('common.orders') || name === 'Orders') return [formatNumber(v), t('common.orders')]
                if (name === t('products.aov') || name === 'AOV') return [formatCurrency(v), t('products.aov')]
                return [String(v), String(name)]
              }}
            />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Bar
              yAxisId="left"
              dataKey="orders"
              name={t('common.orders')}
              fill="#8B5CF6"
              radius={[4, 4, 0, 0]}
              fillOpacity={0.8}
            />
            <Line
              yAxisId="right"
              dataKey="aov"
              name={t('products.aov')}
              stroke="#F59E0B"
              strokeWidth={2}
              dot={{ fill: '#F59E0B', r: 4 }}
            />
          </ComposedChart>
        </ResponsiveContainer>
      )}
    </div>
  )
})
