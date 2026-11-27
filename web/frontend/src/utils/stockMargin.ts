/**
 * The stock-margin block's rules that are not React: which rows a view holds,
 * how a column sorts, which bucket a margin is in, how its numbers read, and
 * its CSV. Every figure is the server's (`core/stock_margin.py`); this file
 * only narrows, orders and prints them.
 */
import type { TFunction } from 'i18next'
import i18n, { LANGUAGE_LOCALES, type SupportedLanguage } from '../lib/i18n'
import type { StockMarginBucket, StockMarginFlag, StockMarginItem } from '../types/api'
import { formatCurrency, formatCurrencyShort, formatNumber } from './formatters'

/** The flags that are a problem; the "Проверить" count is the goods with any. */
export const PROBLEM_FLAGS: readonly StockMarginFlag[] = [
  'no_price', 'no_cost', 'cost_over_price', 'high_margin', 'sold_below_cost', 'only_free',
]
/** Problems that lose money rather than miss data: red, with ▼. */
export const LOSS_FLAGS: readonly StockMarginFlag[] = ['cost_over_price', 'sold_below_cost']

export type StockMarginView = 'goods' | 'problems' | 'slow' | 'nodata' | 'other'
export const VIEWS: readonly StockMarginView[] = ['goods', 'problems', 'slow', 'nodata', 'other']

export const hasProblem = (item: StockMarginItem): boolean =>
  item.flags.some((f) => PROBLEM_FLAGS.includes(f))

export function inView(item: StockMarginItem, view: StockMarginView): boolean {
  switch (view) {
    case 'goods': return item.kind === 'goods'
    case 'problems': return item.kind === 'goods' && hasProblem(item)
    case 'slow': return item.slow
    case 'nodata': return item.kind === 'goods' && !item.costed
    case 'other': return item.kind !== 'goods'
  }
}

/** A ratio as the page shows it — a tenth of a percent, half up — which is how
 * the server files a margin in its bucket (`stock_margin.shown`), so a bar and
 * the rows it lists never disagree. */
export const shownRatio = (ratio: number): number => Math.floor(ratio * 1000 + 0.5) / 1000

export function inBucket(margin: number, bucket: Pick<StockMarginBucket, 'from' | 'to'>): boolean {
  const x = shownRatio(margin)
  return (bucket.from == null || x >= bucket.from) && (bucket.to == null || x < bucket.to)
}

export interface StockMarginFilters {
  view: StockMarginView
  brand: string
  query: string
  /** Index into the response's buckets, or none. */
  bucket: number | null
}

export const NO_FILTERS: StockMarginFilters = { view: 'goods', brand: '', query: '', bucket: null }

export function matchesQuery(item: StockMarginItem, query: string): boolean {
  const q = query.trim().toLowerCase()
  if (!q) return true
  return item.name.toLowerCase().includes(q) || String(item.sku).toLowerCase().includes(q)
}

/** Brand and search narrow every view, and the count on its chip. */
export function narrowed(items: StockMarginItem[], brand: string, query: string): StockMarginItem[] {
  return items.filter((i) => (!brand || (i.brand ?? '') === brand) && matchesQuery(i, query))
}

export function visibleItems(
  items: StockMarginItem[],
  filters: StockMarginFilters,
  buckets: StockMarginBucket[],
): StockMarginItem[] {
  const rows = narrowed(items, filters.brand, filters.query).filter((i) => inView(i, filters.view))
  const bucket = filters.bucket == null ? undefined : buckets[filters.bucket]
  if (!bucket) return rows
  return rows.filter((i) => i.costed && i.margin != null && inBucket(i.margin, bucket))
}

export type StockMarginSortKey =
  | 'name' | 'available' | 'price' | 'cost' | 'margin' | 'realized'
  | 'moved' | 'days' | 'costValue' | 'profit'
export type StockMarginSortDir = 'asc' | 'desc'

/** What a column sorts by. Margin and money exist only for costed goods: a
 * certificate's "margin" is not one, so it sorts with the rows that have none. */
export function sortValue(item: StockMarginItem, key: StockMarginSortKey): number | string | null {
  switch (key) {
    case 'name': return item.name.toLowerCase()
    case 'available': return item.available
    case 'price': return item.price
    case 'cost': return item.cost
    case 'margin': return item.costed ? item.margin : null
    case 'realized': return item.costed ? item.realized_margin_90d : null
    case 'moved': return item.moved_90d
    case 'days': return item.days_of_supply
    case 'costValue': return item.costed ? item.cost_value : null
    case 'profit': return item.costed ? item.profit : null
  }
}

/** No data sorts last in either direction; ties keep the offer's order, so two
 * renders of one list never swap rows. */
export function sortItems(
  items: StockMarginItem[],
  key: StockMarginSortKey,
  dir: StockMarginSortDir,
): StockMarginItem[] {
  const sign = dir === 'asc' ? 1 : -1
  return [...items].sort((a, b) => {
    const x = sortValue(a, key)
    const y = sortValue(b, key)
    if (x == null && y == null) return a.offer_id - b.offer_id
    if (x == null) return 1
    if (y == null) return -1
    if (x < y) return -sign
    if (x > y) return sign
    return a.offer_id - b.offer_id
  })
}

// ─── Numbers ─────────────────────────────────────────────────────────────────
//
// `formatCurrency(-980)` reads "₴-980"; these put the minus first, as a minus
// sign (U+2212), and round before deciding it, so nothing prints "−0".

const MINUS = '−'

function locale(): string {
  return LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'en-US'
}

/** Whole hryvnia: stock values, profit. */
export function money(value: number | null | undefined): string {
  if (value == null) return '—'
  const rounded = Math.round(value)
  return rounded < 0 ? `${MINUS}${formatCurrency(-rounded)}` : formatCurrency(rounded)
}

/** A unit price keeps its kopecks when it has any: ₴7.50 matters on a ₴1 line. */
export function unitPrice(value: number | null | undefined): string {
  if (value == null) return '—'
  const size = Math.abs(value)
  const text = Number.isInteger(size)
    ? formatCurrency(size)
    : `₴${new Intl.NumberFormat(locale(), { minimumFractionDigits: 2, maximumFractionDigits: 2 }).format(size)}`
  return value < 0 ? `${MINUS}${text}` : text
}

/** The dashboard's short form for a tile: "₴8,1 млн". */
export function moneyShort(value: number | null | undefined): string {
  if (value == null) return '—'
  return value < 0 ? `${MINUS}${formatCurrencyShort(-value)}` : formatCurrencyShort(value)
}

/** A ratio (0.25) as a percent with one decimal ("25.0%"), the page's style. */
export function percent(ratio: number | null | undefined, decimals = 1): string {
  if (ratio == null) return '—'
  const factor = 10 ** decimals
  const x = Math.round(ratio * 100 * factor) / factor
  return `${x < 0 ? MINUS : ''}${Math.abs(x).toFixed(decimals)}%`
}

export function units(value: number | null | undefined): string {
  return value == null ? '—' : formatNumber(Math.round(value))
}

/** A bucket's edges as whole percents: [0.4, 0.5) → "40%", "50%". */
export function bucketEdges(bucket: Pick<StockMarginBucket, 'from' | 'to'>): {
  from: string | null
  to: string | null
} {
  const pct = (v: number | null) => (v == null ? null : `${Math.round(v * 100)}%`)
  return { from: pct(bucket.from), to: pct(bucket.to) }
}

/** A bucket's row label: "< 0%", "40%–50%", "60% and above". */
export function bucketLabel(bucket: Pick<StockMarginBucket, 'from' | 'to'>, t: TFunction): string {
  const { from, to } = bucketEdges(bucket)
  if (from == null) return t('margin.stock.dist.under', { to })
  if (to == null) return t('margin.stock.dist.over', { from })
  return t('margin.stock.dist.range', { from, to })
}

/** "2026-11-20" → "20.11.2026". */
export const dayMonthYear = (iso: string): string => iso.split('-').reverse().join('.')

// ─── CSV ─────────────────────────────────────────────────────────────────────

export function toCsv(rows: (string | number | null | undefined)[][]): string {
  const cell = (v: string | number | null | undefined): string => {
    if (v == null) return ''
    const s = String(v)
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s
  }
  return rows.map((r) => r.map(cell).join(',')).join('\n')
}

/** The rows on screen as a CSV, headed in the reader's language. Percent
 * columns are numbers (25.0), money is hryvnia, an empty cell is "no data". */
export function stockMarginCsv(
  rows: StockMarginItem[],
  t: TFunction,
  vars: { days: number; high: string },
): string {
  const pct = (v: number | null) => (v == null ? null : Math.round(v * 1000) / 10)
  const head = [
    t('margin.stock.col.name'), 'SKU', t('margin.stock.d.brand'), t('margin.stock.d.category'),
    t('margin.stock.col.available'), t('margin.stock.d.reserve'), t('margin.stock.d.price'),
    t('margin.stock.d.cost'), `${t('margin.stock.col.margin')}, %`,
    `${t('margin.stock.col.realized', vars)}, %`, t('margin.stock.col.moved', vars),
    t('margin.stock.col.days'), t('margin.stock.col.costValue'), t('margin.stock.col.profit'),
    t('margin.stock.col.flags'),
  ]
  const body = rows.map((i) => [
    i.name, i.sku, i.brand, i.category, i.available, i.reserve, i.price, i.cost,
    i.costed ? pct(i.margin) : null, i.costed ? pct(i.realized_margin_90d) : null, i.moved_90d,
    i.days_of_supply == null ? null : Math.round(i.days_of_supply),
    i.costed ? i.cost_value : null, i.costed ? i.profit : null,
    [...i.flags.map((f) => t(`margin.stock.flagLong.${f}`, vars)),
      ...(i.kind === 'goods' ? [] : [t(`margin.stock.kind.${i.kind}`)])].join('; '),
  ])
  return toCsv([head, ...body])
}
