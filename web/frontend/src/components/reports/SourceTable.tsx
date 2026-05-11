import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import type { ReportSourceRow } from '../../types/api'
import { formatCurrency, formatNumber } from '../../utils/formatters'

export const SourceTable = memo(function SourceTable({ sources }: { sources: ReportSourceRow[] }) {
  const { t } = useTranslation()

  if (!sources.length) {
    return <p className="text-sm text-slate-500 py-8 text-center">{t('reports.noDataPeriod')}</p>
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200 text-left">
            <th className="py-2.5 px-3 font-semibold text-slate-600">{t('traffic.source')}</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">{t('reports.orders')}</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">{t('reports.productsSold')}</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">{t('reports.revenue')}</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden md:table-cell">{t('reports.avgCheck')}</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden lg:table-cell">{t('reports.returnsCount')}</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden lg:table-cell">{t('reports.returnPct')}</th>
          </tr>
        </thead>
        <tbody>
          {sources.map((s) => (
            <tr key={s.source_id} className="border-b border-slate-100 hover:bg-slate-50/50">
              <td className="py-2.5 px-3 font-medium text-slate-800">{s.source_name}</td>
              <td className="py-2.5 px-3 text-right tabular-nums">{formatNumber(s.orders_count)}</td>
              <td className="py-2.5 px-3 text-right tabular-nums hidden sm:table-cell">{formatNumber(s.products_sold)}</td>
              <td className="py-2.5 px-3 text-right tabular-nums font-medium">{formatCurrency(s.revenue)}</td>
              <td className="py-2.5 px-3 text-right tabular-nums hidden md:table-cell">{formatCurrency(s.avg_check)}</td>
              <td className="py-2.5 px-3 text-right tabular-nums hidden lg:table-cell">{s.returns_count}</td>
              <td className="py-2.5 px-3 text-right tabular-nums hidden lg:table-cell">{s.return_rate}%</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
})
