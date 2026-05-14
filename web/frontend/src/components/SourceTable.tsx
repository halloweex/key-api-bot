import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import type { ReportSourceRow } from '../types/api'
import { formatCurrency, formatNumber } from '../utils/formatters'
import { DataTable, Tr, Th, Td } from './DataTable'

export const SourceTable = memo(function SourceTable({ sources }: { sources: ReportSourceRow[] }) {
  const { t } = useTranslation()

  if (!sources.length) {
    return <p className="text-sm text-slate-500 py-8 text-center">{t('reports.noDataPeriod')}</p>
  }

  return (
    <DataTable variant="feature">
      <thead>
        <Tr header>
          <Th>{t('traffic.source')}</Th>
          <Th align="right">{t('reports.orders')}</Th>
          <Th align="right" hideBelow="sm">{t('reports.productsSold')}</Th>
          <Th align="right">{t('reports.revenue')}</Th>
          <Th align="right" hideBelow="md">{t('reports.avgCheck')}</Th>
          <Th align="right" hideBelow="lg">{t('reports.returnsCount')}</Th>
          <Th align="right" hideBelow="lg">{t('reports.returnPct')}</Th>
        </Tr>
      </thead>
      <tbody>
        {sources.map((s) => (
          <Tr key={s.source_id}>
            <Td bold>{s.source_name}</Td>
            <Td align="right" tabular>{formatNumber(s.orders_count)}</Td>
            <Td align="right" tabular hideBelow="sm">{formatNumber(s.products_sold)}</Td>
            <Td align="right" tabular bold>{formatCurrency(s.revenue)}</Td>
            <Td align="right" tabular hideBelow="md">{formatCurrency(s.avg_check)}</Td>
            <Td align="right" tabular hideBelow="lg">{s.returns_count}</Td>
            <Td align="right" tabular hideBelow="lg">{s.return_rate}%</Td>
          </Tr>
        ))}
      </tbody>
    </DataTable>
  )
})
