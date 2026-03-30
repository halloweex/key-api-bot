import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { ChevronUp, ChevronDown } from 'lucide-react'
import { ChartContainer } from '../charts/ChartContainer'
import { useMarginBrandCategory } from '../../hooks'
import { formatCurrency, formatPercent } from '../../utils/formatters'
import type { MarginBrandCategoryItem } from '../../types/api'

// ─── Types ───────────────────────────────────────────────────────────────────

type SortField = 'brand' | 'category' | 'total_revenue' | 'profit' | 'margin_pct'
type SortDir = 'asc' | 'desc'

// ─── Component ───────────────────────────────────────────────────────────────

export const MarginBrandCategoryTable = memo(function MarginBrandCategoryTable() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useMarginBrandCategory()

  const [sortField, setSortField] = useState<SortField>('total_revenue')
  const [sortDir, setSortDir] = useState<SortDir>('desc')

  const handleSort = (field: SortField) => {
    if (sortField === field) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortField(field)
      setSortDir('desc')
    }
  }

  const sortedData = useMemo(() => {
    if (!data?.length) return []
    return [...data].sort((a, b) => {
      const aVal = a[sortField] ?? 0
      const bVal = b[sortField] ?? 0
      if (typeof aVal === 'string' && typeof bVal === 'string') {
        return sortDir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal)
      }
      return sortDir === 'asc' ? Number(aVal) - Number(bVal) : Number(bVal) - Number(aVal)
    })
  }, [data, sortField, sortDir])

  const isEmpty = !isLoading && sortedData.length === 0

  const SortIcon = ({ field }: { field: SortField }) => {
    if (sortField !== field) return <ChevronDown className="w-3 h-3 text-slate-300" />
    return sortDir === 'asc'
      ? <ChevronUp className="w-3 h-3 text-purple-600" />
      : <ChevronDown className="w-3 h-3 text-purple-600" />
  }

  const ThButton = ({ field, children }: { field: SortField; children: React.ReactNode }) => (
    <th className="py-2 px-3">
      <button
        onClick={() => handleSort(field)}
        className="flex items-center gap-1 text-xs font-semibold text-slate-500 uppercase hover:text-slate-700"
      >
        {children}
        <SortIcon field={field} />
      </button>
    </th>
  )

  return (
    <ChartContainer
      title={t('margin.brandCategory')}
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      isEmpty={isEmpty}
      height="auto"
      ariaLabel={t('margin.brandCategoryDesc')}
    >
      <div className="overflow-x-auto max-h-[400px] overflow-y-auto">
        <table className="w-full text-sm">
          <thead className="sticky top-0 bg-white">
            <tr className="border-b border-slate-200">
              <ThButton field="brand">{t('margin.brand')}</ThButton>
              <ThButton field="category">{t('margin.category')}</ThButton>
              <ThButton field="total_revenue">{t('margin.revenue')}</ThButton>
              <ThButton field="profit">{t('margin.grossProfit')}</ThButton>
              <ThButton field="margin_pct">{t('margin.marginPct')}</ThButton>
              <th className="py-2 px-3 text-xs font-semibold text-slate-500 uppercase text-right">
                {t('margin.costCoverage')}
              </th>
            </tr>
          </thead>
          <tbody>
            {sortedData.map((item: MarginBrandCategoryItem, idx: number) => (
              <tr
                key={`${item.brand}-${item.category}`}
                className={`border-b border-slate-100 hover:bg-slate-50 ${idx % 2 === 0 ? '' : 'bg-slate-25'}`}
              >
                <td className="py-1.5 px-3 font-medium text-slate-900 whitespace-nowrap">
                  {item.brand}
                </td>
                <td className="py-1.5 px-3 text-slate-600 whitespace-nowrap">
                  {item.category}
                </td>
                <td className="py-1.5 px-3 text-right text-slate-700">
                  {formatCurrency(item.total_revenue)}
                </td>
                <td className="py-1.5 px-3 text-right font-medium text-green-700">
                  {formatCurrency(item.profit)}
                </td>
                <td className="py-1.5 px-3 text-right">
                  {item.margin_pct != null ? (
                    <span className={`inline-flex px-2 py-0.5 rounded-full text-xs font-semibold ${
                      item.margin_pct >= 40
                        ? 'bg-green-100 text-green-700'
                        : item.margin_pct >= 25
                          ? 'bg-yellow-100 text-yellow-700'
                          : 'bg-red-100 text-red-700'
                    }`}>
                      {formatPercent(item.margin_pct)}
                    </span>
                  ) : (
                    <span className="text-slate-400">N/A</span>
                  )}
                </td>
                <td className="py-1.5 px-3 text-right text-slate-500">
                  {formatPercent(item.coverage_pct)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </ChartContainer>
  )
})
