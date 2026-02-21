import { memo, useState, useCallback } from 'react'
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card'
import { SkeletonChart, ApiErrorState } from '../ui'
import { useReportSummary, useReportTopProducts } from '../../hooks/useApi'
import { useQueryParams } from '../../store/filterStore'
import { formatCurrency, formatNumber } from '../../utils/formatters'
import type { ReportSourceRow, ReportTopProduct } from '../../types/api'

// ─── Types ────────────────────────────────────────────────────────────────────

type Tab = 'summary' | 'top_products'

const SOURCE_FILTERS = [
  { id: null, label: 'All' },
  { id: 1, label: 'Instagram' },
  { id: 4, label: 'Shopify' },
  { id: 2, label: 'Telegram' },
] as const

const LIMIT_OPTIONS = [10, 25, 50] as const

// ─── CSV Download ─────────────────────────────────────────────────────────────

function useDownloadCsv() {
  const queryParams = useQueryParams()

  return useCallback(
    (type: 'summary' | 'top_products', extra?: string) => {
      const params = new URLSearchParams(queryParams)
      params.set('type', type)
      if (extra) {
        const extraParams = new URLSearchParams(extra)
        extraParams.forEach((v, k) => params.set(k, v))
      }
      window.open(`/api/reports/export/csv?${params.toString()}`, '_blank')
    },
    [queryParams],
  )
}

// ─── Summary Tab ──────────────────────────────────────────────────────────────

const MetricCard = memo(function MetricCard({
  label,
  value,
  sub,
}: {
  label: string
  value: string
  sub?: string
}) {
  return (
    <div className="bg-white rounded-xl border border-slate-200/60 shadow-[var(--shadow-card)] p-4">
      <p className="text-xs text-slate-500 font-medium mb-1">{label}</p>
      <p className="text-xl font-bold text-slate-900 tracking-tight">{value}</p>
      {sub && <p className="text-xs text-slate-400 mt-0.5">{sub}</p>}
    </div>
  )
})

const SourceTable = memo(function SourceTable({ sources }: { sources: ReportSourceRow[] }) {
  if (!sources.length) {
    return <p className="text-sm text-slate-500 py-8 text-center">No data for this period</p>
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200 text-left">
            <th className="py-2.5 px-3 font-semibold text-slate-600">Source</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">Orders</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">Products</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">Revenue</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden md:table-cell">Avg Check</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden lg:table-cell">Returns</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden lg:table-cell">Return %</th>
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

const SummaryTab = memo(function SummaryTab() {
  const { data, isLoading, error, refetch } = useReportSummary()
  const downloadCsv = useDownloadCsv()

  if (isLoading) return <SkeletonChart />
  if (error) return <ApiErrorState error={error} onRetry={refetch} title="Failed to load summary" />
  if (!data) return null

  const { totals } = data

  return (
    <div className="space-y-4">
      {/* Totals Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <MetricCard label="Orders" value={formatNumber(totals.orders_count)} />
        <MetricCard label="Revenue" value={formatCurrency(totals.revenue)} />
        <MetricCard label="Products Sold" value={formatNumber(totals.products_sold)} />
        <MetricCard
          label="Avg Check"
          value={formatCurrency(totals.avg_check)}
          sub={`${totals.returns_count} returns (${totals.return_rate}%)`}
        />
      </div>

      {/* Source Table */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle>Source Breakdown</CardTitle>
          <button
            onClick={() => downloadCsv('summary')}
            className="text-xs font-medium text-purple-600 hover:text-purple-700 bg-purple-50 hover:bg-purple-100 px-3 py-1.5 rounded-lg transition-colors"
          >
            Export CSV
          </button>
        </CardHeader>
        <CardContent className="!p-0 sm:!px-5 sm:!pb-5">
          <SourceTable sources={data.sources} />
        </CardContent>
      </Card>
    </div>
  )
})

// ─── Top Products Tab ─────────────────────────────────────────────────────────

const MEDAL_STYLES: Record<number, string> = {
  1: 'bg-amber-50 text-amber-700 border-amber-200',
  2: 'bg-slate-50 text-slate-600 border-slate-300',
  3: 'bg-orange-50 text-orange-700 border-orange-200',
}

const ProductsTable = memo(function ProductsTable({
  products,
}: {
  products: ReportTopProduct[]
}) {
  if (!products.length) {
    return <p className="text-sm text-slate-500 py-8 text-center">No data for this period</p>
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-slate-200 text-left">
            <th className="py-2.5 px-3 font-semibold text-slate-600 w-10">#</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600">Product</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">Qty</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">%</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">Revenue</th>
            <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden md:table-cell">Orders</th>
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

const TopProductsTab = memo(function TopProductsTab() {
  const [sourceFilter, setSourceFilter] = useState<number | null>(null)
  const [limit, setLimit] = useState<number>(10)
  const { data, isLoading, error, refetch } = useReportTopProducts(sourceFilter, limit)
  const downloadCsv = useDownloadCsv()

  if (isLoading) return <SkeletonChart />
  if (error) return <ApiErrorState error={error} onRetry={refetch} title="Failed to load products" />

  return (
    <div className="space-y-4">
      <Card>
        <CardHeader className="flex flex-col sm:flex-row sm:items-center gap-3">
          <CardTitle className="flex-1">Top Products</CardTitle>
          <div className="flex items-center gap-2 flex-wrap">
            {/* Source filter chips */}
            <div className="flex gap-1">
              {SOURCE_FILTERS.map((sf) => (
                <button
                  key={sf.label}
                  onClick={() => setSourceFilter(sf.id)}
                  className={`px-2.5 py-1 rounded-lg text-xs font-medium transition-colors ${
                    sourceFilter === sf.id
                      ? 'bg-purple-100 text-purple-700'
                      : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
                  }`}
                >
                  {sf.label}
                </button>
              ))}
            </div>

            {/* Limit selector */}
            <select
              value={limit}
              onChange={(e) => setLimit(Number(e.target.value))}
              className="text-xs border border-slate-200 rounded-lg px-2 py-1 bg-white text-slate-600"
            >
              {LIMIT_OPTIONS.map((l) => (
                <option key={l} value={l}>
                  Top {l}
                </option>
              ))}
            </select>

            {/* CSV download */}
            <button
              onClick={() => {
                const extra = new URLSearchParams()
                if (sourceFilter) extra.set('source_id', String(sourceFilter))
                extra.set('limit', String(limit))
                downloadCsv('top_products', extra.toString())
              }}
              className="text-xs font-medium text-purple-600 hover:text-purple-700 bg-purple-50 hover:bg-purple-100 px-3 py-1.5 rounded-lg transition-colors"
            >
              Export CSV
            </button>
          </div>
        </CardHeader>
        <CardContent className="!p-0 sm:!px-5 sm:!pb-5">
          <ProductsTable products={data ?? []} />
        </CardContent>
      </Card>
    </div>
  )
})

// ─── Main Page ────────────────────────────────────────────────────────────────

const TABS: { key: Tab; label: string }[] = [
  { key: 'summary', label: 'Summary' },
  { key: 'top_products', label: 'Top Products' },
]

export const ReportsPage = memo(function ReportsPage() {
  const [activeTab, setActiveTab] = useState<Tab>('summary')

  return (
    <main className="flex-1 py-3 px-1 sm:py-4 sm:px-1.5 lg:py-6 lg:px-2 overflow-auto">
      <div className="max-w-[1800px] mx-auto space-y-4 sm:space-y-6">
        {/* Tab bar */}
        <div className="flex gap-1 bg-slate-100 p-1 rounded-xl w-fit">
          {TABS.map((tab) => (
            <button
              key={tab.key}
              onClick={() => setActiveTab(tab.key)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                activeTab === tab.key
                  ? 'bg-white text-slate-900 shadow-sm'
                  : 'text-slate-500 hover:text-slate-700'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Tab content */}
        {activeTab === 'summary' && <SummaryTab />}
        {activeTab === 'top_products' && <TopProductsTab />}
      </div>
    </main>
  )
})

export default ReportsPage
