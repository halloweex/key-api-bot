import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import type { ReportTopProduct } from '../types/api'
import { formatCurrency, formatNumber } from '../utils/formatters'

const MEDAL_STYLES: Record<number, string> = {
  1: 'bg-amber-50 text-amber-700 border-amber-200',
  2: 'bg-slate-50 text-slate-600 border-slate-300',
  3: 'bg-orange-50 text-orange-700 border-orange-200',
}

export const ProductsTable = memo(function ProductsTable({
  products,
}: {
  products: ReportTopProduct[]
}) {
  const { t } = useTranslation()

  if (!products.length) {
    return <p className="text-sm text-slate-500 py-8 text-center">{t('reports.noDataPeriod')}</p>
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200 text-left">
            <th className="py-2.5 px-3 font-semibold text-slate-600 w-10">{t('reports.rank')}</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600">{t('reports.product')}</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">{t('reports.qty')}</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">{t('reports.pct')}</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">{t('reports.revenue')}</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden md:table-cell">{t('reports.orders')}</th>
          </tr>
        </thead>
        <tbody>
          {products.map((p) => {
            const medal = MEDAL_STYLES[p.rank]
            return (
              <tr
                key={`${p.rank}-${p.sku}`}
                className={`border-b border-slate-100 hover:bg-slate-50/50 ${medal ? 'font-medium' : ''}`}
              >
                <td className="py-2.5 px-3">
                  {medal ? (
                    <span className={`inline-flex items-center justify-center w-6 h-6 rounded-full border text-xs font-bold ${medal}`}>
                      {p.rank}
                    </span>
                  ) : (
                    <span className="text-slate-400 text-xs">{p.rank}</span>
                  )}
                </td>
                <td className="py-2.5 px-3">
                  <div className="min-w-0">
                    <p className={`truncate max-w-[300px] ${medal ? 'text-slate-900' : 'text-slate-800'}`}>
                      {p.product_name}
                    </p>
                    {p.sku && (
                      <p className="text-[11px] text-slate-400 truncate">{p.sku}</p>
                    )}
                  </div>
                </td>
                <td className="py-2.5 px-3 text-right tabular-nums font-semibold">{formatNumber(p.quantity)}</td>
                <td className="py-2.5 px-3 text-right tabular-nums text-slate-500">{p.percentage}%</td>
                <td className="py-2.5 px-3 text-right tabular-nums hidden sm:table-cell">{formatCurrency(p.revenue)}</td>
                <td className="py-2.5 px-3 text-right tabular-nums hidden md:table-cell">{formatNumber(p.orders_count)}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
})
