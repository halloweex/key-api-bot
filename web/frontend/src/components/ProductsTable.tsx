import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import type { ReportTopProduct } from '../types/api'
import { formatCurrency, formatNumber } from '../utils/formatters'
import { DataTable, Tr, Th, Td } from './DataTable'

// Top-3 rank medal styles (gold / silver / bronze). Single-use inline visual.
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
    <DataTable variant="feature">
      <thead>
        <Tr header>
          <Th>{t('reports.rank')}</Th>
          <Th>{t('reports.product')}</Th>
          <Th align="right">{t('reports.qty')}</Th>
          <Th align="right">{t('reports.pct')}</Th>
          <Th align="right" hideBelow="sm">{t('reports.revenue')}</Th>
          <Th align="right" hideBelow="md">{t('reports.orders')}</Th>
        </Tr>
      </thead>
      <tbody>
        {products.map((p) => {
          const medal = MEDAL_STYLES[p.rank]
          return (
            <Tr key={`${p.rank}-${p.sku}`}>
              <Td>
                {medal ? (
                  <span className={`inline-flex items-center justify-center w-6 h-6 rounded-full border text-xs font-bold ${medal}`}>
                    {p.rank}
                  </span>
                ) : (
                  <span className="text-slate-400 text-xs">{p.rank}</span>
                )}
              </Td>
              <Td>
                <div className="min-w-0">
                  <p className={`truncate max-w-[300px] ${medal ? 'text-slate-900 font-medium' : 'text-slate-800'}`}>
                    {p.product_name}
                  </p>
                  {p.sku && (
                    <p className="text-[11px] text-slate-400 truncate">{p.sku}</p>
                  )}
                </div>
              </Td>
              <Td align="right" tabular>
                <span className="font-semibold">{formatNumber(p.quantity)}</span>
              </Td>
              <Td align="right" tabular>
                <span className="text-slate-500">{p.percentage}%</span>
              </Td>
              <Td align="right" tabular hideBelow="sm">{formatCurrency(p.revenue)}</Td>
              <Td align="right" tabular hideBelow="md">{formatNumber(p.orders_count)}</Td>
            </Tr>
          )
        })}
      </tbody>
    </DataTable>
  )
})
