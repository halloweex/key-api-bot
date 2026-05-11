import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card'
import { SkeletonChart, ApiErrorState } from '../ui'
import { Wrapper } from '../Wrapper'
import { MetricCard } from '../MetricCard'
import { useReportSummary } from '../../hooks/useApi'
import { formatCurrency, formatNumber } from '../../utils/formatters'
import { SourceTable } from './SourceTable'
import { useDownloadCsv } from './useDownloadCsv'

export const SummaryTab = memo(function SummaryTab() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useReportSummary()
  const downloadCsv = useDownloadCsv()

  if (isLoading) return <SkeletonChart />
  if (error) return <ApiErrorState error={error} onRetry={refetch} title="Failed to load summary" />
  if (!data) return null

  const { totals } = data

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <MetricCard label={t('reports.orders')} value={formatNumber(totals.orders_count)} />
        <MetricCard label={t('reports.revenue')} value={formatCurrency(totals.revenue)} />
        <MetricCard label={t('reports.productsSold')} value={formatNumber(totals.products_sold)} />
        <MetricCard
          label={t('reports.avgCheck')}
          value={formatCurrency(totals.avg_check)}
          sub={`${totals.returns_count} ${t('reports.returnsCount')} (${totals.return_rate}%)`}
        />
      </div>

      <Card>
        <CardHeader>
          <Wrapper dir="row" align="center" justify="between">
            <CardTitle>{t('reports.sourceBreakdown')}</CardTitle>
            <button
              onClick={() => downloadCsv('summary')}
              className="text-xs font-medium text-purple-600 hover:text-purple-700 bg-purple-50 hover:bg-purple-100 px-3 py-1.5 rounded-lg transition-colors"
            >
              {t('reports.exportCsv')}
            </button>
          </Wrapper>
        </CardHeader>
        <CardContent padding="table">
          <SourceTable sources={data.sources} />
        </CardContent>
      </Card>
    </div>
  )
})
