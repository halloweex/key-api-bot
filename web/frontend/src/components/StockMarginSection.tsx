import { memo, useCallback, useMemo, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Lock } from 'lucide-react'
import { Badge } from './Badge'
import { ChartContainer } from './ChartContainer'
import { HelpSheet } from './HelpSheet'
import { StockMarginSummary } from './StockMarginSummary'
import { StockMarginTable } from './StockMarginTable'
import { useFileExport } from './useFileExport'
import { useStockMargin } from '../hooks'
import { useIsAdmin } from '../hooks/useAuth'
import { LANGUAGE_LOCALES, type SupportedLanguage } from '../lib/i18n'
import { saveCsv } from '../utils/deliverFile'
import {
  NO_FILTERS, percent, stockMarginCsv,
  type StockMarginFilters, type StockMarginView,
} from '../utils/stockMargin'
import type { StockMarginItem, StockMarginResponse } from '../types/api'

// ─── StockMarginSection ──────────────────────────────────────────────────────
//
// The margin of everything in stock, the last section of /margin. Admins only
// (owner's decision, 2026-11-26): for anybody else it renders nothing at all —
// no heading, no placeholder and no request, since /margin itself can be
// ticked for a non-admin and the server refuses them this block anyway
// (web/routes/api/stock_margin.py). It owns the query, the filters the
// summary and the table share, and the CSV export; the two halves only draw.

// The stock syncs hourly: two missed syncs and the stamp turns amber.
const STALE_AFTER_MS = 2 * 60 * 60 * 1000

export const StockMarginSection = memo(function StockMarginSection() {
  const isAdmin = useIsAdmin()
  if (!isAdmin) return null
  return <ForAdmin />
})

function ForAdmin() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useStockMargin(true)
  const [filters, setFilters] = useState<StockMarginFilters>(NO_FILTERS)
  const table = useRef<HTMLElement>(null)
  const { run: runExport, busy: exporting } = useFileExport()

  const show = useCallback((view: StockMarginView) => {
    // A shortcut from the summary lists the whole view, not the current narrowing.
    setFilters({ ...NO_FILTERS, view })
    table.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }, [])
  const pickBucket = useCallback((bucket: number | null) => {
    setFilters((f) => ({ ...f, view: 'goods', bucket }))
    if (bucket != null) table.current?.scrollIntoView({ behavior: 'smooth', block: 'start' })
  }, [])
  const vars = useMemo(() => ({
    days: data?.window_days ?? 90,
    high: percent(data?.thresholds.high_margin ?? 0.85, 0),
  }), [data])
  const exportRows = useCallback((rows: StockMarginItem[]) => {
    void runExport(() => saveCsv(stockMarginCsv(rows, t, vars), 'stock-margin.csv'))
  }, [runExport, t, vars])

  const isEmpty = !isLoading && !error && data != null && data.items.length === 0
  return (
    <>
      <section aria-label={t('margin.stock.title')}>
        <ChartContainer
          title={t('margin.stock.title')}
          titleExtra={<Stamp snapshotAt={data?.snapshot_at ?? null} />}
          action={data ? <Help data={data} /> : undefined}
          isLoading={isLoading}
          error={error}
          onRetry={refetch}
          isEmpty={isEmpty}
          emptyMessage={t('margin.stock.empty')}
          height="auto"
        >
          {data && (
            <StockMarginSummary data={data} bucket={filters.bucket} onBucket={pickBucket} onShow={show} />
          )}
        </ChartContainer>
      </section>
      {data && data.items.length > 0 && (
        <section aria-label={t('margin.stock.tableTitle')} ref={table} className="scroll-mt-4">
          <StockMarginTable
            data={data}
            filters={filters}
            onFilters={setFilters}
            onExport={exportRows}
            exporting={exporting}
          />
        </section>
      )}
    </>
  )
}

/** "admins only", and when the stock rows were last rebuilt — amber once late. */
function Stamp({ snapshotAt }: { snapshotAt: string | null }) {
  const { t, i18n } = useTranslation()
  const admin = <Badge tone="purple" icon={Lock}>{t('margin.stock.adminOnly')}</Badge>
  if (!snapshotAt) return admin
  const at = new Date(snapshotAt)
  const time = at.toLocaleString(LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'en-US', {
    timeZone: 'Europe/Kyiv', day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit',
  })
  const stale = Date.now() - at.getTime() > STALE_AFTER_MS
  return (
    <span className="inline-flex flex-wrap items-center gap-1.5">
      {admin}
      <Badge tone={stale ? 'orange' : 'neutral'}>
        {t(stale ? 'margin.stock.staleAsOf' : 'margin.stock.asOf', { time })}
      </Badge>
    </span>
  )
}

/** What every figure means, read once and kept under "?". */
function Help({ data }: { data: StockMarginResponse }) {
  const { t } = useTranslation()
  const days = data.window_days
  return (
    <HelpSheet title={t('margin.stock.howTitle')} label={t('margin.stock.howTitle')}>
      <div className="space-y-2 text-sm leading-relaxed text-slate-700">
        <p>{t('margin.stock.how.margin')}</p>
        <p>{t('margin.stock.how.weighted')}</p>
        <p>{t('margin.stock.how.stock')}</p>
        <p>{t('margin.stock.how.realized', { days })}</p>
        <p>{t('margin.stock.how.supply', { days })}</p>
        <p>{t('margin.stock.how.excluded')}</p>
        <p>
          {t('margin.stock.how.colours', {
            low: percent(data.thresholds.low_margin, 0),
            high: percent(data.thresholds.high_margin, 0),
          })}
        </p>
      </div>
    </HelpSheet>
  )
}
