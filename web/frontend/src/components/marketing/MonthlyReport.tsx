import { memo, useMemo, useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card'
import { SkeletonChart, ApiErrorState } from '../ui'
import { useMarketingReport } from '../../hooks/useApi'
import { useFilterStore } from '../../store/filterStore'
import { formatCurrency, formatNumber } from '../../utils/formatters'
import i18n, { LANGUAGE_LOCALES, type SupportedLanguage } from '../../lib/i18n'
import type { MarketingMonthStats, MarketingBrandRow, MarketingSourceRow } from '../../types/api'

// ─── Helpers ──────────────────────────────────────────────────────────────

function formatDateRange(startDate: string, endDate: string): string {
  const locale = LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'uk-UA'
  const start = new Date(startDate + 'T00:00:00')
  const end = new Date(endDate + 'T00:00:00')

  // Check if it's a full calendar month
  const sameMonth = start.getMonth() === end.getMonth() && start.getFullYear() === end.getFullYear()
  const isFirstDay = start.getDate() === 1
  const lastDayOfMonth = new Date(end.getFullYear(), end.getMonth() + 1, 0).getDate()
  const isLastDay = end.getDate() === lastDayOfMonth

  if (sameMonth && isFirstDay && isLastDay) {
    // Full month — show "APRIL 2026"
    return new Intl.DateTimeFormat(locale, { month: 'long', year: 'numeric' }).format(start).toUpperCase()
  }

  // Range — show "01.04 – 10.04.2026"
  const fmtStart = new Intl.DateTimeFormat(locale, { day: '2-digit', month: '2-digit' }).format(start)
  const fmtEnd = new Intl.DateTimeFormat(locale, { day: '2-digit', month: '2-digit', year: 'numeric' }).format(end)
  return `${fmtStart} – ${fmtEnd}`
}

function pctChange(cur: number, prev: number): string {
  if (prev === 0) return cur > 0 ? '+100%' : '0%'
  const pct = ((cur - prev) / prev) * 100
  const sign = pct > 0 ? '+' : ''
  return `${sign}${pct.toFixed(1)}%`
}

function changeColor(cur: number, prev: number): string {
  if (cur > prev) return 'text-emerald-600'
  if (cur < prev) return 'text-red-500'
  return 'text-slate-500'
}

function formatGoal(value: number | null, t: (k: string) => string): string {
  if (value == null) return '—'
  if (value >= 1_000_000) {
    const m = value / 1_000_000
    return `\u20B4${m % 1 === 0 ? m.toFixed(0) : m.toFixed(1)} ${t('marketing.mln')}`
  }
  return formatCurrency(value)
}

// ─── Section 1: General Sales ─────────────────────────────────────────────

const GENERAL_ROWS: {
  key: keyof MarketingMonthStats
  labelKey: string
  format: (v: number) => string
  showGoal?: boolean
}[] = [
  { key: 'revenue', labelKey: 'marketing.revenue', format: formatCurrency, showGoal: true },
  { key: 'orders', labelKey: 'marketing.orders', format: formatNumber },
  { key: 'avg_check', labelKey: 'marketing.avgCheck', format: formatCurrency },
  { key: 'customers', labelKey: 'marketing.customers', format: formatNumber },
  { key: 'new_customers', labelKey: 'marketing.newCustomers', format: formatNumber },
  { key: 'returning_customers', labelKey: 'marketing.returningCustomers', format: formatNumber },
  { key: 'return_rate', labelKey: 'marketing.returnRate', format: (v) => `${v.toFixed(0)}%` },
]

const GeneralSalesSection = memo(function GeneralSalesSection({
  current, previous, yearAgo, monthlyGoal,
  prevLabel, yoyLabel,
}: {
  current: MarketingMonthStats
  previous: MarketingMonthStats
  yearAgo: MarketingMonthStats
  monthlyGoal: number | null
  prevLabel: string
  yoyLabel: string
}) {
  const { t } = useTranslation()

  return (
    <Card>
      <CardHeader>
        <CardTitle>{t('marketing.generalSales')}</CardTitle>
      </CardHeader>
      <CardContent className="!p-0 sm:!px-5 sm:!pb-5">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-left">
                <th className="py-2.5 px-3 font-semibold text-slate-600">{t('marketing.metric')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">{t('marketing.currentPeriod')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">{prevLabel}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">{t('marketing.change')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden lg:table-cell">{yoyLabel}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden lg:table-cell">{t('marketing.changeYoY')}</th>
                {monthlyGoal != null && (
                  <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden md:table-cell">{t('marketing.monthGoal')}</th>
                )}
              </tr>
            </thead>
            <tbody>
              {GENERAL_ROWS.map((row) => {
                const curVal = current[row.key]
                const prevVal = previous[row.key]
                const yoyVal = yearAgo[row.key]
                return (
                  <tr key={row.key} className="border-b border-slate-100 hover:bg-slate-50/50">
                    <td className="py-2.5 px-3 font-medium text-slate-800">{t(row.labelKey)}</td>
                    <td className="py-2.5 px-3 text-right tabular-nums font-semibold text-slate-900">
                      {row.format(curVal)}
                    </td>
                    <td className="py-2.5 px-3 text-right tabular-nums text-slate-500 hidden sm:table-cell">
                      {row.format(prevVal)}
                    </td>
                    <td className={`py-2.5 px-3 text-right tabular-nums font-medium hidden sm:table-cell ${changeColor(curVal, prevVal)}`}>
                      {pctChange(curVal, prevVal)}
                    </td>
                    <td className="py-2.5 px-3 text-right tabular-nums text-slate-500 hidden lg:table-cell">
                      {row.format(yoyVal)}
                    </td>
                    <td className={`py-2.5 px-3 text-right tabular-nums font-medium hidden lg:table-cell ${changeColor(curVal, yoyVal)}`}>
                      {pctChange(curVal, yoyVal)}
                    </td>
                    {monthlyGoal != null && (
                      <td className="py-2.5 px-3 text-right tabular-nums text-slate-500 hidden md:table-cell">
                        {row.showGoal ? formatGoal(monthlyGoal, t) : ''}
                      </td>
                    )}
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  )
})

// ─── Section 2: Brands ────────────────────────────────────────────────────

const BrandsSection = memo(function BrandsSection({ brands }: { brands: MarketingBrandRow[] }) {
  const { t } = useTranslation()

  return (
    <Card>
      <CardHeader>
        <CardTitle>{t('marketing.salesByBrand')}</CardTitle>
      </CardHeader>
      <CardContent className="!p-0 sm:!px-5 sm:!pb-5">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-left">
                <th className="py-2.5 px-3 font-semibold text-slate-600">{t('marketing.brand')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">{t('marketing.revenue')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">{t('marketing.orders')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">{t('marketing.avgCheck')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">{t('marketing.shareOfTotal')}</th>
              </tr>
            </thead>
            <tbody>
              {brands.map((b) => (
                <tr key={b.brand} className="border-b border-slate-100 hover:bg-slate-50/50">
                  <td className="py-2.5 px-3 font-medium text-slate-800">{b.brand}</td>
                  <td className="py-2.5 px-3 text-right tabular-nums font-semibold">{formatCurrency(b.revenue)}</td>
                  <td className="py-2.5 px-3 text-right tabular-nums hidden sm:table-cell">{formatNumber(b.orders)}</td>
                  <td className="py-2.5 px-3 text-right tabular-nums hidden sm:table-cell">{formatCurrency(b.avg_check)}</td>
                  <td className="py-2.5 px-3 text-right tabular-nums text-slate-500">{b.share_pct}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  )
})

// ─── Section 3: Sources ───────────────────────────────────────────────────

const SourcesSection = memo(function SourcesSection({ sources }: { sources: MarketingSourceRow[] }) {
  const { t } = useTranslation()

  return (
    <Card>
      <CardHeader>
        <CardTitle>{t('marketing.channels')}</CardTitle>
      </CardHeader>
      <CardContent className="!p-0 sm:!px-5 sm:!pb-5">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-200 text-left">
                <th className="py-2.5 px-3 font-semibold text-slate-600">{t('marketing.channel')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">{t('marketing.orders')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">{t('marketing.revenue')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">{t('marketing.ordersPct')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">{t('marketing.revenuePct')}</th>
              </tr>
            </thead>
            <tbody>
              {sources.map((s) => (
                <tr key={s.source_name} className="border-b border-slate-100 hover:bg-slate-50/50">
                  <td className="py-2.5 px-3 font-medium text-slate-800">{s.source_name}</td>
                  <td className="py-2.5 px-3 text-right tabular-nums">{formatNumber(s.orders)}</td>
                  <td className="py-2.5 px-3 text-right tabular-nums font-semibold">{formatCurrency(s.revenue)}</td>
                  <td className="py-2.5 px-3 text-right tabular-nums text-slate-500 hidden sm:table-cell">{s.orders_pct}%</td>
                  <td className="py-2.5 px-3 text-right tabular-nums text-slate-500 hidden sm:table-cell">{s.revenue_pct}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </CardContent>
    </Card>
  )
})

// ─── Main Component ───────────────────────────────────────────────────────

export const MonthlyReport = memo(function MonthlyReport() {
  const { t } = useTranslation()
  const salesType = useFilterStore((s) => s.salesType)
  const period = useFilterStore((s) => s.period)
  const startDate = useFilterStore((s) => s.startDate)
  const endDate = useFilterStore((s) => s.endDate)

  const { data, isLoading, error, refetch } = useMarketingReport()

  const downloadCsv = useCallback(() => {
    const params = new URLSearchParams({ sales_type: salesType })
    if (period !== 'custom') {
      params.set('period', period)
    } else if (startDate && endDate) {
      params.set('start_date', startDate)
      params.set('end_date', endDate)
    }
    window.open(`/api/reports/marketing-summary/export/csv?${params}`, '_blank')
  }, [period, startDate, endDate, salesType])

  // Dynamic labels for comparison columns
  const { periodLabel, prevLabel, yoyLabel } = useMemo(() => {
    if (!data) return { periodLabel: '', prevLabel: '', yoyLabel: '' }
    return {
      periodLabel: formatDateRange(data.start_date, data.end_date),
      prevLabel: formatDateRange(data.prev_start_date, data.prev_end_date),
      yoyLabel: formatDateRange(data.yoy_start_date, data.yoy_end_date),
    }
  }, [data])

  return (
    <div className="space-y-4">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-3">
          <h2 className="text-lg font-bold text-slate-800">
            {t('marketing.monthlyReport')}
          </h2>
          {data && (
            <span className="text-sm text-slate-500 font-medium">
              {periodLabel}
            </span>
          )}
        </div>
        <button
          onClick={downloadCsv}
          className="text-xs font-medium text-purple-600 hover:text-purple-700 bg-purple-50 hover:bg-purple-100 px-3 py-1.5 rounded-lg transition-colors"
        >
          {t('reports.exportCsv')}
        </button>
      </div>

      {isLoading && <SkeletonChart />}
      {error && <ApiErrorState error={error} onRetry={refetch} title="Failed to load report" />}
      {data && (
        <>
          <GeneralSalesSection
            current={data.general_sales.current}
            previous={data.general_sales.previous}
            yearAgo={data.general_sales.year_ago}
            monthlyGoal={data.general_sales.monthly_goal}
            prevLabel={prevLabel}
            yoyLabel={yoyLabel}
          />
          <BrandsSection brands={data.brands} />
          <SourcesSection sources={data.sources} />
        </>
      )}
    </div>
  )
})
