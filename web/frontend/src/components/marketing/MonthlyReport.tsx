import { memo, useState, useMemo, useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card'
import { SkeletonChart, ApiErrorState } from '../ui'
import { useMarketingReport } from '../../hooks/useApi'
import { useFilterStore } from '../../store/filterStore'
import { formatCurrency, formatNumber } from '../../utils/formatters'
import i18n, { LANGUAGE_LOCALES, type SupportedLanguage } from '../../lib/i18n'
import type { MarketingMonthStats, MarketingBrandRow, MarketingSourceRow } from '../../types/api'

// ─── Helpers ──────────────────────────────────────────────────────────────

function getMonthName(year: number, month: number): string {
  const locale = LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'uk-UA'
  const d = new Date(year, month - 1, 1)
  return new Intl.DateTimeFormat(locale, { month: 'long' }).format(d).toUpperCase()
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

// ─── Month Selector ───────────────────────────────────────────────────────

const MonthSelector = memo(function MonthSelector({
  year, month, onChange,
}: {
  year: number
  month: number
  onChange: (y: number, m: number) => void
}) {
  const prev = () => {
    if (month === 1) onChange(year - 1, 12)
    else onChange(year, month - 1)
  }
  const next = () => {
    if (month === 12) onChange(year + 1, 1)
    else onChange(year, month + 1)
  }

  const label = useMemo(() => `${getMonthName(year, month)} ${year}`, [year, month])

  return (
    <div className="flex items-center gap-3">
      <button
        onClick={prev}
        className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-500 transition-colors"
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M15 19l-7-7 7-7" />
        </svg>
      </button>
      <span className="text-sm font-bold text-slate-800 min-w-[160px] text-center">
        {label}
      </span>
      <button
        onClick={next}
        className="p-1.5 rounded-lg hover:bg-slate-100 text-slate-500 transition-colors"
      >
        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
        </svg>
      </button>
    </div>
  )
})

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
}: {
  current: MarketingMonthStats
  previous: MarketingMonthStats
  yearAgo: MarketingMonthStats
  monthlyGoal: number | null
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
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right">{t('marketing.currentMonth')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">{t('marketing.previousMonth')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden sm:table-cell">{t('marketing.change')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden lg:table-cell">{t('marketing.yearAgo')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden lg:table-cell">{t('marketing.changeYoY')}</th>
                <th className="py-2.5 px-3 font-semibold text-slate-600 text-right hidden md:table-cell">{t('marketing.monthGoal')}</th>
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
                    <td className="py-2.5 px-3 text-right tabular-nums text-slate-500 hidden md:table-cell">
                      {row.showGoal ? formatGoal(monthlyGoal, t) : ''}
                    </td>
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
  const now = new Date()
  const [year, setYear] = useState(now.getFullYear())
  const [month, setMonth] = useState(now.getMonth() + 1)
  const salesType = useFilterStore((s) => s.salesType)

  const { data, isLoading, error, refetch } = useMarketingReport(year, month)

  const downloadCsv = useCallback(() => {
    const params = new URLSearchParams({
      year: String(year),
      month: String(month),
      months: '3',
      sales_type: salesType,
    })
    window.open(`/api/reports/marketing-summary/export/csv?${params}`, '_blank')
  }, [year, month, salesType])

  return (
    <div className="space-y-4">
      {/* Header with month selector and export */}
      <div className="flex items-center justify-between flex-wrap gap-2">
        <h2 className="text-lg font-bold text-slate-800">
          {t('marketing.monthlyReport')}
        </h2>
        <div className="flex items-center gap-3">
          <button
            onClick={downloadCsv}
            className="text-xs font-medium text-purple-600 hover:text-purple-700 bg-purple-50 hover:bg-purple-100 px-3 py-1.5 rounded-lg transition-colors"
          >
            {t('reports.exportCsv')}
          </button>
          <MonthSelector year={year} month={month} onChange={(y, m) => { setYear(y); setMonth(m) }} />
        </div>
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
          />
          <BrandsSection brands={data.brands} />
          <SourcesSection sources={data.sources} />
        </>
      )}
    </div>
  )
})
