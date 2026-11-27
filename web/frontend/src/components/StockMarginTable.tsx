import { Fragment, memo, useMemo, useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { Search } from 'lucide-react'
import { Badge } from './Badge'
import { BottomSheet } from './BottomSheet'
import { Button } from './Button'
import { ChartContainer } from './ChartContainer'
import { DataTable, SortableTh, Td, Th, Tr } from './DataTable'
import { FilterChip } from './FilterChip'
import { Input } from './Input'
import { Select } from './Select'
import { useIsPhone } from '../hooks/useMediaQuery'
import { formatNumber } from '../utils/formatters'
import {
  LOSS_FLAGS, NO_FILTERS, PROBLEM_FLAGS, VIEWS, bucketLabel, dayMonthYear, inView, money, moneyShort,
  narrowed, percent, shownRatio, sortItems, unitPrice, units, visibleItems,
  type StockMarginFilters, type StockMarginSortDir, type StockMarginSortKey,
} from '../utils/stockMargin'
import type { StockMarginItem, StockMarginResponse } from '../types/api'

// ─── StockMarginTable ────────────────────────────────────────────────────────
//
// Every offer in stock, one row each: the lower half of the stock-margin
// block (StockMarginSection owns the data, the shared filters and the CSV).
// On a wide screen a table whose product column stays put while the numbers
// scroll; on a phone a card per offer, fifty at a time. A tap opens the
// offer's whole record beside the table — a side panel, a bottom sheet on a
// phone (NN/g: a side panel, not an inline accordion). Brand and search narrow
// every view and its count. No data sorts last in either direction, and a
// margin under the threshold is a badge, a negative one a ▼, never colour
// alone.

const PHONE_PAGE = 50

type Vars = { days: number; high: string }

interface StockMarginTableProps {
  data: StockMarginResponse
  filters: StockMarginFilters
  onFilters: (filters: StockMarginFilters) => void
  /** Deliver the rows on screen as a CSV. */
  onExport: (rows: StockMarginItem[]) => void
  exporting?: boolean
}

export const StockMarginTable = memo(function StockMarginTable({
  data, filters, onFilters, onExport, exporting = false,
}: StockMarginTableProps) {
  const { t } = useTranslation()
  const isPhone = useIsPhone()
  const [sortKey, setSortKey] = useState<StockMarginSortKey>('profit')
  const [sortDir, setSortDir] = useState<StockMarginSortDir>('desc')
  const [limit, setLimit] = useState(PHONE_PAGE)
  const [openId, setOpenId] = useState<number | null>(null)

  const { items, thresholds } = data
  const buckets = data.goods.buckets
  const low = thresholds.low_margin
  const vars: Vars = useMemo(
    () => ({ days: data.window_days, high: percent(thresholds.high_margin, 0) }),
    [data.window_days, thresholds.high_margin],
  )

  const brands = useMemo(
    () => [...new Set(items.map((i) => i.brand).filter((b): b is string => !!b))].sort((a, b) => a.localeCompare(b)),
    [items],
  )
  const base = useMemo(() => narrowed(items, filters.brand, filters.query), [items, filters.brand, filters.query])
  const rows = useMemo(
    () => sortItems(visibleItems(items, filters, buckets), sortKey, sortDir),
    [items, filters, buckets, sortKey, sortDir],
  )
  const total = useMemo(() => items.filter((i) => inView(i, filters.view)).length, [items, filters.view])
  const costedRows = rows.filter((i) => i.costed)
  const open = openId == null ? null : items.find((i) => i.offer_id === openId) ?? null

  const set = (patch: Partial<StockMarginFilters>) => {
    onFilters({ ...filters, ...patch })
    setLimit(PHONE_PAGE)
  }
  const onSort = (column: string) => {
    const key = column as StockMarginSortKey
    if (key === sortKey) {
      setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    } else {
      setSortKey(key)
      setSortDir(key === 'name' ? 'asc' : 'desc')
    }
  }

  const labels: Record<StockMarginSortKey, string> = {
    name: t('margin.stock.col.name'),
    available: t('margin.stock.col.available'),
    price: t('margin.stock.col.price'),
    cost: t('margin.stock.col.cost'),
    margin: t('margin.stock.col.margin'),
    realized: t('margin.stock.col.realized', vars),
    moved: t('margin.stock.col.moved', vars),
    days: t('margin.stock.col.days'),
    costValue: t('margin.stock.col.costValue'),
    profit: t('margin.stock.col.profit'),
  }
  const numeric: StockMarginSortKey[] = [
    'available', 'price', 'cost', 'margin', 'realized', 'moved', 'days', 'costValue', 'profit',
  ]
  const phoneSorts: StockMarginSortKey[] = [
    'profit', 'margin', 'realized', 'days', 'costValue', 'moved', 'available', 'price', 'name',
  ]

  const active: string[] = []
  const pickedBucket = filters.bucket == null ? undefined : buckets[filters.bucket]
  if (pickedBucket) active.push(t('margin.stock.filterBucket', { range: bucketLabel(pickedBucket, t) }))
  if (filters.brand) active.push(t('margin.stock.filterBrand', { brand: filters.brand }))
  if (filters.query.trim()) active.push(t('margin.stock.filterQuery', { query: filters.query.trim() }))

  return (
    <ChartContainer
      title={t('margin.stock.tableTitle')}
      height="auto"
      action={(
        <Button variant="secondary" size="sm" onClick={() => onExport(rows)} disabled={exporting || rows.length === 0}>
          {t('margin.stock.csv')}
        </Button>
      )}
    >
      <div className="flex flex-wrap items-center gap-2">
        <Input
          type="search"
          width={isPhone ? 'full' : 'search'}
          value={filters.query}
          onChange={(v) => set({ query: v })}
          placeholder={t('margin.stock.search')}
          aria-label={t('margin.stock.search')}
          prefix={<Search className="h-4 w-4" aria-hidden />}
        />
        <Select
          options={brands.map((b) => ({ value: b, label: b }))}
          value={filters.brand || null}
          onChange={(v) => set({ brand: v ?? '' })}
          allowEmpty
          emptyLabel={t('margin.stock.allBrands')}
          aria-label={t('margin.stock.brand')}
        />
        {isPhone && (
          <Select
            options={phoneSorts.flatMap((k) => (['desc', 'asc'] as const).map((d) => ({
              value: `${k}:${d}`, label: `${labels[k]} ${d === 'desc' ? '↓' : '↑'}`,
            })))}
            value={`${sortKey}:${sortDir}`}
            onChange={(v) => {
              if (!v) return
              const [k, d] = v.split(':')
              setSortKey(k as StockMarginSortKey)
              setSortDir(d as StockMarginSortDir)
            }}
            aria-label={t('margin.stock.sort')}
          />
        )}
      </div>

      <div className="mt-3 flex gap-2 overflow-x-auto pb-1 sm:flex-wrap" role="group" aria-label={t('margin.stock.tableTitle')}>
        {VIEWS.map((v) => (
          // A chip keeps its width: on a phone the row scrolls sideways rather
          // than folding "No price or cost" onto four lines.
          <span key={v} className="shrink-0 whitespace-nowrap">
            <FilterChip active={filters.view === v} onClick={() => set({ view: v, bucket: null })} size={isPhone ? 'md' : 'sm'}>
              {t(`margin.stock.view.${v}`)}{' '}
              <span className="tabular-nums opacity-70">{formatNumber(base.filter((i) => inView(i, v)).length)}</span>
            </FilterChip>
          </span>
        ))}
      </div>

      {active.length > 0 && (
        <p className="mt-2 text-xs text-slate-500">
          {t('margin.stock.filters', { list: active.join(' · ') })}{' '}
          <button
            type="button"
            onClick={() => set({ brand: '', query: '', bucket: null })}
            className="font-medium text-purple-700 underline underline-offset-2"
          >
            {t('margin.stock.reset')}
          </button>
        </p>
      )}

      <div className="mt-3">
        {rows.length === 0 ? (
          <p className="py-6 text-center text-sm text-slate-500">
            {t('margin.stock.nothing')}{' '}
            <button
              type="button"
              onClick={() => { onFilters(NO_FILTERS); setLimit(PHONE_PAGE) }}
              className="font-medium text-purple-700 underline underline-offset-2"
            >
              {t('margin.stock.resetAll')}
            </button>
          </p>
        ) : isPhone ? (
          <>
            <ul className="space-y-2">
              {rows.slice(0, limit).map((i) => (
                <li key={i.offer_id}>
                  <Card item={i} low={low} vars={vars} onOpen={() => setOpenId(i.offer_id)} />
                </li>
              ))}
            </ul>
            {rows.length > limit && (
              <div className="mt-3">
                <Button variant="secondary" fullWidth onClick={() => setLimit((l) => l + PHONE_PAGE)}>
                  {t('margin.stock.more', { n: formatNumber(Math.min(PHONE_PAGE, rows.length - limit)) })}
                </Button>
              </div>
            )}
          </>
        ) : (
          <DataTable stickyHeader>
            <thead>
              <Tr header sticky variant="admin">
                <SortableTh pinned column="name" label={labels.name} sortBy={sortKey} sortDir={sortDir} onSort={onSort} />
                {numeric.map((k) => (
                  <SortableTh key={k} column={k} label={labels[k]} sortBy={sortKey} sortDir={sortDir} onSort={onSort} align="right" />
                ))}
                <Th>{t('margin.stock.col.flags')}</Th>
              </Tr>
            </thead>
            <tbody>
              {rows.map((i) => (
                <Tr key={i.offer_id} onClick={() => setOpenId(i.offer_id)}>
                  <Td pinned>
                    <button
                      type="button"
                      onClick={(e) => { e.stopPropagation(); setOpenId(i.offer_id) }}
                      className="block min-w-[11rem] max-w-[20rem] text-left"
                    >
                      <span className={`line-clamp-2 font-medium ${i.available === 0 ? 'text-slate-500' : 'text-slate-800'}`}>
                        {i.name}
                      </span>
                      <span className="block text-xs text-slate-500">{[i.brand, i.sku].filter(Boolean).join(' · ')}</span>
                    </button>
                  </Td>
                  <Td align="right" tabular>{units(i.available)}</Td>
                  <Td align="right" tabular>
                    {i.price == null ? <Badge tone="orange">{t('margin.stock.none')}</Badge> : unitPrice(i.price)}
                  </Td>
                  <Td align="right" tabular>
                    {i.cost == null
                      ? (i.kind === 'goods' ? <Badge tone="orange">{t('margin.stock.none')}</Badge> : '—')
                      : unitPrice(i.cost)}
                  </Td>
                  <Td align="right" tabular><Margin value={i.costed ? i.margin : null} low={low} /></Td>
                  <Td align="right" tabular><Margin value={i.costed ? i.realized_margin_90d : null} low={low} /></Td>
                  <Td align="right" tabular>{units(i.moved_90d)}</Td>
                  <Td align="right" tabular><Days item={i} slowDays={thresholds.slow_days} /></Td>
                  <Td align="right" tabular>{i.costed ? money(i.cost_value) : '—'}</Td>
                  <Td align="right" tabular>{i.costed ? money(i.profit) : '—'}</Td>
                  <Td><Flags item={i} vars={vars} /></Td>
                </Tr>
              ))}
            </tbody>
          </DataTable>
        )}
      </div>

      <p className="mt-3 text-xs text-slate-500">
        {t('margin.stock.shown', { shown: formatNumber(rows.length), total: formatNumber(total) })}
        {costedRows.length > 0 && ` · ${t('margin.stock.shownProfit', {
          profit: moneyShort(costedRows.reduce((s, i) => s + (i.profit ?? 0), 0)),
        })}`}
      </p>

      <BottomSheet
        placement="side"
        open={open != null}
        title={open?.name ?? ''}
        onClose={() => setOpenId(null)}
        closeLabel={t('margin.stock.close')}
      >
        {open && <Detail item={open} low={low} slowDays={thresholds.slow_days} vars={vars} />}
      </BottomSheet>
    </ChartContainer>
  )
})

/** A margin: "—" for none, a badge under the threshold, ▼ when negative. */
function Margin({ value, low }: { value: number | null; low: number }) {
  if (value == null) return <span className="text-slate-400">—</span>
  if (value < -1) {
    return <span className="font-semibold text-red-700"><span aria-hidden="true">▼ </span>&lt; {percent(-1, 0)}</span>
  }
  const shown = shownRatio(value)
  if (shown < 0) {
    return <span className="font-semibold text-red-700"><span aria-hidden="true">▼ </span>{percent(value)}</span>
  }
  if (shown < low) return <Badge tone="orange">{percent(value)}</Badge>
  return <span className="font-semibold text-slate-800">{percent(value)}</span>
}

/** Days of supply, "no sales" when nothing left the stock, "over a year" past it. */
function Days({ item, slowDays }: { item: StockMarginItem; slowDays: number }) {
  const { t } = useTranslation()
  const days = item.days_of_supply
  if (days == null) {
    return <span className="whitespace-nowrap text-slate-400">{item.available > 0 ? t('margin.stock.noSales') : '—'}</span>
  }
  if (days > slowDays) return <span className="whitespace-nowrap text-amber-700">{t('margin.stock.overYear')}</span>
  return <>{units(days)}</>
}

/** The row's flags — a loss red with ▼, missing data amber with !, the rest plain. */
function Flags({ item, vars, long = false }: { item: StockMarginItem; vars: Vars; long?: boolean }) {
  const { t } = useTranslation()
  if (item.flags.length === 0 && item.kind === 'goods') return null
  return (
    <span className="flex flex-wrap gap-1">
      {item.flags.map((f) => {
        const loss = LOSS_FLAGS.includes(f)
        const problem = PROBLEM_FLAGS.includes(f)
        return (
          <Badge key={f} tone={loss ? 'red' : problem ? 'orange' : 'neutral'}>
            {loss ? '▼ ' : problem ? '! ' : ''}
            {t(`margin.stock.${long ? 'flagLong' : 'flag'}.${f}`, vars)}
          </Badge>
        )
      })}
      {item.kind !== 'goods' && <Badge tone="neutral">{t(`margin.stock.kind.${item.kind}`)}</Badge>}
    </span>
  )
}

/** One offer on a phone: name and margin, then what is available, the profit on it and what left. */
function Card({ item, low, vars, onOpen }: { item: StockMarginItem; low: number; vars: Vars; onOpen: () => void }) {
  const { t } = useTranslation()
  const flagged = item.flags.length > 0 || item.kind !== 'goods'
  return (
    <button
      type="button"
      onClick={onOpen}
      className={`w-full rounded-xl border border-slate-200 p-3 text-left
                  ${item.available === 0 ? 'bg-slate-50' : 'bg-white'}`}
    >
      <span className="flex items-start justify-between gap-2">
        <span className="line-clamp-2 text-sm font-medium text-slate-800">{item.name}</span>
        {item.costed && <span className="shrink-0 text-sm tabular-nums"><Margin value={item.margin} low={low} /></span>}
      </span>
      <span className="block text-xs text-slate-500">{[item.brand, item.sku].filter(Boolean).join(' · ')}</span>
      <span className="mt-2 grid grid-cols-[repeat(3,auto)] justify-between gap-x-3 text-xs tabular-nums">
        <Figure label={t('margin.stock.col.available')} value={units(item.available)} />
        <Figure label={t('margin.stock.card.profit')} value={item.costed ? moneyShort(item.profit) : '—'} />
        <Figure label={t('margin.stock.col.moved', vars)} value={units(item.moved_90d)} />
      </span>
      {flagged && <span className="mt-2 block"><Flags item={item} vars={vars} /></span>}
    </button>
  )
}

function Figure({ label, value }: { label: string; value: string }) {
  return (
    <span>
      <span className="block whitespace-nowrap text-slate-500">{label}</span>
      <span className="block text-sm font-semibold text-slate-800">{value}</span>
    </span>
  )
}

/** The offer's whole record, in the panel beside the table. */
function Detail({ item, low, slowDays, vars }: { item: StockMarginItem; low: number; slowDays: number; vars: Vars }) {
  const { t } = useTranslation()
  const days = item.days_of_supply
  const daysText = days == null
    ? '—'
    : days > slowDays
      ? `${t('margin.stock.overYear')} (${t('margin.stock.daysValue', { n: units(days) })})`
      : t('margin.stock.daysValue', { n: units(days) })
  const sections: [string, ReactNode][][] = [
    [
      [t('margin.stock.d.brand'), item.brand ?? '—'],
      [t('margin.stock.d.category'),
        [item.category, item.root_category !== item.category ? item.root_category : null].filter(Boolean).join(' · ') || '—'],
      [t('margin.stock.d.sku'), item.sku],
    ],
    [
      [t('margin.stock.d.price'), item.price == null ? t('margin.stock.none') : unitPrice(item.price)],
      [t('margin.stock.d.cost'), item.cost == null ? t('margin.stock.none') : unitPrice(item.cost)],
      [t('margin.stock.d.unitProfit'), unitPrice(item.unit_profit)],
      [t('margin.stock.d.margin'), <Margin value={item.margin} low={low} />],
      [t('margin.stock.d.markup'), percent(item.markup, 0)],
    ],
    [
      [t('margin.stock.d.quantity'), units(item.quantity)],
      [t('margin.stock.d.reserve'), units(item.reserve)],
      [t('margin.stock.d.available'), units(item.available)],
      [t('margin.stock.d.retail'), money(item.retail_value)],
      [t('margin.stock.d.costValue'), money(item.cost_value)],
      [t('margin.stock.d.profit'), money(item.profit)],
    ],
    [
      [t('margin.stock.d.moved', vars), t('margin.stock.pcs', { n: units(item.moved_90d) })],
      [t('margin.stock.d.units', vars), t('margin.stock.pcs', { n: units(item.units_90d) })],
      [t('margin.stock.d.revenue', vars), money(item.revenue_90d)],
      [t('margin.stock.d.avgPrice'), unitPrice(item.avg_price_90d)],
      [t('margin.stock.d.discount'), percent(item.discount_90d)],
      [t('margin.stock.d.realized'), <Margin value={item.realized_margin_90d} low={low} />],
      [t('margin.stock.d.days'), daysText],
      [t('margin.stock.d.lastSale'), item.last_sale_date ? dayMonthYear(item.last_sale_date) : '—'],
    ],
  ]
  return (
    <div>
      <Flags item={item} vars={vars} long />
      {sections.map((rows, s) => (
        <dl
          key={s}
          className={`grid grid-cols-[1fr_auto] gap-x-3 gap-y-1.5 py-3 text-sm tabular-nums
                      ${s > 0 ? 'border-t border-slate-100' : ''}`}
        >
          {rows.map(([label, value]) => (
            <Fragment key={label}>
              <dt className="text-slate-500">{label}</dt>
              <dd className="text-right text-slate-800">{value}</dd>
            </Fragment>
          ))}
        </dl>
      ))}
      <p className="text-xs text-slate-500">{t('margin.stock.d.note')}</p>
    </div>
  )
}
