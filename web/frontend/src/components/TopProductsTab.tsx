import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Card, CardHeader, CardTitle, CardContent } from './Card'
import { SkeletonChart } from './Skeleton'
import { ApiErrorState } from './ApiErrorState'
import { Input } from './Input'
import { Select } from './Select'
import { ExportCsvButton } from './ExportCsvButton'
import { FilterChip } from './FilterChip'
import { Wrapper } from './Wrapper'
import { useReportTopProducts, useReportAllProducts } from '../hooks/useApi'
import type { ReportTopProductsResponse } from '../types/api'
import { ProductsTable } from './ProductsTable'
import { useDownloadCsv } from './useDownloadCsv'

const SOURCE_FILTERS = [
  { id: null, label: 'All' },
  { id: 1, label: 'Instagram' },
  { id: 4, label: 'Shopify' },
  { id: 2, label: 'Telegram' },
] as const

const LIMIT_OPTIONS = [10, 25, 50, 0] as const

export const TopProductsTab = memo(function TopProductsTab() {
  const { t } = useTranslation()
  const [sourceFilter, setSourceFilter] = useState<number | null>(null)
  const [limit, setLimit] = useState<number>(10)
  const [search, setSearch] = useState('')
  const isAll = limit === 0

  const topQuery = useReportTopProducts(sourceFilter, isAll ? 10 : limit)
  const allQuery = useReportAllProducts(sourceFilter)

  const activeQuery = isAll ? allQuery : topQuery
  const { isLoading, error, refetch } = activeQuery
  const data: ReportTopProductsResponse | undefined = activeQuery.data

  const downloadCsv = useDownloadCsv()

  if (isLoading) return <SkeletonChart />
  if (error) return <ApiErrorState error={error} onRetry={refetch} title="Failed to load products" />

  const allProducts = data ?? []
  const filtered = search
    ? allProducts.filter(
        (p) =>
          p.product_name.toLowerCase().includes(search.toLowerCase()) ||
          (p.sku && p.sku.toLowerCase().includes(search.toLowerCase())),
      )
    : allProducts

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader>
          <Wrapper dir="row-responsive" align="start" gap="md">
            <Wrapper flex={1}>
              <CardTitle>{t('reports.topProducts')}</CardTitle>
            </Wrapper>
            <Wrapper dir="row" align="center" gap="sm" wrap>
              <Input
                type="text"
                size="sm"
                width="search"
                value={search}
                onChange={setSearch}
                placeholder={t('reports.searchProducts')}
              />

              <Wrapper dir="row" gap="xs">
                {SOURCE_FILTERS.map((sf) => (
                  <FilterChip
                    key={sf.label}
                    active={sourceFilter === sf.id}
                    onClick={() => setSourceFilter(sf.id)}
                  >
                    {sf.label}
                  </FilterChip>
                ))}
              </Wrapper>

              <Select
                variant="compact"
                options={LIMIT_OPTIONS.map((l) => ({
                  value: String(l),
                  label: l === 0 ? t('reports.allProducts') : `Top ${l}`,
                }))}
                value={String(limit)}
                onChange={(v) => { if (v) { setLimit(Number(v)); setSearch('') } }}
                allowEmpty={false}
              />

              <ExportCsvButton
                onClick={() => {
                  const extra = new URLSearchParams()
                  if (sourceFilter) extra.set('source_id', String(sourceFilter))
                  extra.set('limit', isAll ? '5000' : String(limit))
                  downloadCsv('top_products', extra.toString())
                }}
              >
                {t('reports.exportCsv')}
              </ExportCsvButton>
            </Wrapper>
          </Wrapper>
        </CardHeader>
        <CardContent padding="table">
          {search && (
            <p className="text-xs text-slate-400 px-5 sm:px-0 pb-2">
              {t('reports.showingCount', { count: filtered.length, total: allProducts.length })}
            </p>
          )}
          <ProductsTable products={filtered} />
        </CardContent>
      </Card>
    </div>
  )
})
