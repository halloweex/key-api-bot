import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { AlertTriangle } from 'lucide-react'
import { ChartContainer } from '../charts/ChartContainer'
import { Badge } from '../Badge'
import { useMarginAlerts } from '../../hooks'
import { formatCurrency, formatPercent } from '../../utils/formatters'

// ─── Component ───────────────────────────────────────────────────────────────

export const MarginAlerts = memo(function MarginAlerts() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useMarginAlerts()

  const isEmpty = !isLoading && (!data || data.length === 0)

  return (
    <ChartContainer
      title={t('margin.alerts')}
      titleExtra={
        data && data.length > 0 ? (
          <Badge tone="red" icon={<AlertTriangle className="w-3 h-3" />}>
            {data.length}
          </Badge>
        ) : null
      }
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      emptyMessage={t('margin.noAlerts')}
      height="auto"
      ariaLabel={t('margin.alertsDesc')}
    >
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-slate-200">
              <th className="text-left py-2 px-3 text-xs font-semibold text-slate-500 uppercase">
                {t('margin.brand')}
              </th>
              <th className="text-right py-2 px-3 text-xs font-semibold text-slate-500 uppercase">
                {t('margin.revenue')}
              </th>
              <th className="text-right py-2 px-3 text-xs font-semibold text-slate-500 uppercase">
                {t('margin.marginPct')}
              </th>
              <th className="text-right py-2 px-3 text-xs font-semibold text-slate-500 uppercase">
                {t('margin.floor')}
              </th>
              <th className="text-right py-2 px-3 text-xs font-semibold text-slate-500 uppercase">
                {t('margin.impact')}
              </th>
            </tr>
          </thead>
          <tbody>
            {data?.map((item) => (
              <tr key={item.brand} className="border-b border-slate-100 hover:bg-slate-50">
                <td className="py-2 px-3 font-medium text-slate-900">
                  {item.brand}
                </td>
                <td className="py-2 px-3 text-right text-slate-600">
                  {formatCurrency(item.total_revenue)}
                </td>
                <td className="py-2 px-3 text-right">
                  <Badge tone={item.margin_pct < 15 ? 'red' : 'yellow'}>
                    {formatPercent(item.margin_pct)}
                  </Badge>
                </td>
                <td className="py-2 px-3 text-right text-slate-500">
                  {formatPercent(item.margin_floor)}
                </td>
                <td className="py-2 px-3 text-right font-semibold text-red-600">
                  {formatCurrency(item.impact)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </ChartContainer>
  )
})
