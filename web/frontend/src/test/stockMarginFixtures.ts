import type {
  StockMarginExcluded, StockMarginFlag, StockMarginItem, StockMarginKind, StockMarginResponse,
} from '../types/api'

// Illustrative stock for the stock-margin block's stories: invented names and
// figures, shaped like the shelf on 2026-11-26 — a best-seller with more than
// a year of stock, a sun stick sold below cost after discounts, a mistyped
// cost, goods without a cost or a price, reserved ones, a gift certificate and
// two giveaways. Production's numbers stay off a public repository. The
// derived fields are computed as core/stock_margin.py computes them.

const DAYS = 90
const LOW = 0.3
const HIGH = 0.85
const SLOW_DAYS = 365
const PROBLEM: StockMarginFlag[] = [
  'no_price', 'no_cost', 'cost_over_price', 'high_margin', 'sold_below_cost', 'only_free',
]
const BUCKETS: [number | null, number | null][] = [
  [null, 0], [0, 0.2], [0.2, 0.3], [0.3, 0.4], [0.4, 0.5], [0.5, 0.6], [0.6, null],
]

export interface ShelfRow {
  id: number
  name: string
  brand?: string | null
  category?: string
  price: number
  cost: number | null
  qty: number
  reserve?: number
  units?: number
  revenue?: number
  moved?: number
  kind?: StockMarginKind
}

function offer(s: ShelfRow): StockMarginItem {
  const price = s.price
  const cost = s.cost != null && s.cost > 0 ? s.cost : null
  const available = Math.max(0, s.qty - (s.reserve ?? 0))
  const kind = s.kind ?? 'goods'
  const costed = kind === 'goods' && cost != null && price > 0
  const units = s.units ?? 0
  const revenue = s.revenue ?? 0
  const moved = s.moved ?? units
  const margin = costed ? (price - cost) / price : null
  const avg = units > 0 ? revenue / units : null
  const realized = costed && units > 0 && revenue > 0 ? (revenue - cost * units) / revenue : null
  const days = available > 0 && moved > 0 ? available / (moved / DAYS) : null
  const slow = costed && available > 0 && (moved <= 0 || (days != null && days > SLOW_DAYS))
  const flags: StockMarginFlag[] = []
  if (kind === 'goods') {
    if (price <= 0) flags.push('no_price')
    else if (cost == null) flags.push('no_cost')
    else if (cost >= price) flags.push('cost_over_price')
    else if (margin != null && margin > HIGH) flags.push('high_margin')
    if (realized != null && realized < 0) flags.push('sold_below_cost')
    if (units > 0 && revenue <= 0) flags.push('only_free')
  }
  if (slow) flags.push('slow')
  if (available === 0) flags.push('reserved')
  return {
    offer_id: s.id,
    product_id: s.id * 10,
    sku: String(1000 + s.id),
    name: s.name,
    brand: s.brand === undefined ? 'Lumiere' : s.brand,
    category: s.category ?? 'Creams',
    root_category: 'Face',
    kind,
    quantity: s.qty,
    reserve: s.reserve ?? 0,
    available,
    price: price > 0 ? price : null,
    cost,
    costed,
    margin,
    markup: costed ? (price - cost) / cost : null,
    unit_profit: costed ? price - cost : null,
    retail_value: price > 0 ? available * price : null,
    cost_value: costed ? available * cost : null,
    profit: costed ? available * (price - cost) : null,
    units_90d: units,
    revenue_90d: revenue,
    avg_price_90d: avg,
    discount_90d: avg != null && price > 0 ? 1 - avg / price : null,
    realized_margin_90d: realized,
    moved_90d: moved,
    days_of_supply: days,
    slow,
    last_sale_date: moved > 0 ? '2026-11-24' : null,
    flags,
  }
}

function excluded(items: StockMarginItem[], kind: StockMarginKind): StockMarginExcluded {
  const rows = items.filter((i) => i.kind === kind)
  return {
    positions: rows.length,
    available: rows.reduce((s, i) => s + i.available, 0),
    retail_value: rows.reduce((s, i) => s + (i.retail_value ?? 0), 0),
  }
}

export const SHELF: ShelfRow[] = [
  { id: 1, name: 'Lumiere Eye Cream EMS Lift 15 ml', price: 1200, cost: 420, qty: 600, units: 90, revenue: 100000, moved: 140 },
  { id: 2, name: 'Hanbit Matcha Foaming Cleanser 200 ml', brand: 'Hanbit', price: 900, cost: 460, qty: 500, units: 300, revenue: 250000, moved: 350 },
  { id: 3, name: 'Sora Berry Pore Mousse 150 ml', brand: 'Sora', price: 850, cost: 310, qty: 300, reserve: 3, units: 60, revenue: 48000, moved: 70 },
  { id: 4, name: 'Daon Vitamin C Serum 22% 30 ml', brand: 'Daon', category: 'Serums', price: 1100, cost: 380, qty: 100, units: 40, revenue: 41000, moved: 45 },
  { id: 5, name: 'Lumiere Cica Sun Stick SPF 50', category: 'Sun care', price: 700, cost: 300, qty: 120, reserve: 3, units: 25, revenue: 6500, moved: 25 },
  { id: 6, name: 'Hanbit Body Scrub 300 ml', brand: 'Hanbit', category: 'Body', price: 1000, cost: 690, qty: 70, units: 15, revenue: 12500, moved: 15 },
  { id: 7, name: 'Sora PDRN Lip Serum', brand: 'Sora', price: 400, cost: 290, qty: 2, reserve: 2, units: 6, revenue: 2300, moved: 6 },
  { id: 8, name: 'Daon Premium Cell Ampoule 10 ml', brand: 'Daon', category: 'Serums', price: 3000, cost: 260, qty: 6, units: 10, revenue: 28000, moved: 10 },
  { id: 9, name: 'Lumiere Love Box: For Me', category: 'Sets', price: 1500, cost: null, qty: 10 },
  { id: 10, name: 'Hanbit PDRN Recovery Cream 70 g', brand: 'Hanbit', price: 0, cost: null, qty: 30 },
  { id: 11, name: 'Sora Hand Mask', brand: 'Sora', category: 'Body', price: 150, cost: 95, qty: 3, reserve: 3, units: 4, revenue: 560, moved: 4 },
  { id: 12, name: 'Gift certificate ₴1000', brand: null, category: 'Certificates', price: 1000, cost: 40, qty: 8, kind: 'certificate' },
  { id: 13, name: 'Daon Vitamin C Serum 10 ml tester', brand: 'Daon', category: 'Serums', price: 450, cost: 30, qty: 150, units: 2, revenue: 450, kind: 'promo' },
  { id: 14, name: 'Mystery beauty box', brand: null, category: 'Gifts', price: 1, cost: 400, qty: 40, units: 100, revenue: 15, kind: 'promo' },
]

/** A shelf with nothing to check. */
export const CLEAN_SHELF: ShelfRow[] = [
  { id: 2, name: 'Hanbit Matcha Foaming Cleanser 200 ml', brand: 'Hanbit', price: 900, cost: 460, qty: 50, units: 300, revenue: 250000, moved: 350 },
  { id: 3, name: 'Sora Berry Pore Mousse 150 ml', brand: 'Sora', price: 850, cost: 310, qty: 25, units: 60, revenue: 48000, moved: 70 },
  { id: 4, name: 'Daon Vitamin C Serum 22% 30 ml', brand: 'Daon', category: 'Serums', price: 1100, cost: 380, qty: 20, units: 40, revenue: 41000, moved: 45 },
]

export function stockMarginFixture(
  shelf: ShelfRow[] = SHELF,
  snapshotAt = '2026-11-26T17:58:18+00:00',
): StockMarginResponse {
  const items = shelf.map(offer)
  const goods = items.filter((i) => i.kind === 'goods')
  const costed = goods.filter((i) => i.costed)
  const sold = costed.filter((i) => i.units_90d > 0)
  const slow = goods.filter((i) => i.slow)
  const sum = (xs: StockMarginItem[], f: (i: StockMarginItem) => number) => xs.reduce((s, i) => s + f(i), 0)
  const retail = sum(costed, (i) => i.available * (i.price ?? 0))
  const cost = sum(costed, (i) => i.available * (i.cost ?? 0))
  const revenue = sum(sold, (i) => i.revenue_90d)
  const atList = sum(sold, (i) => (i.price ?? 0) * i.units_90d)
  const cogs = sum(sold, (i) => (i.cost ?? 0) * i.units_90d)
  const margins = costed.map((i) => i.margin ?? 0).sort((a, b) => a - b)
  const mid = margins.length / 2
  const median = margins.length === 0
    ? null
    : margins.length % 2 ? margins[Math.floor(mid)] : (margins[mid - 1] + margins[mid]) / 2
  const shown = (r: number) => Math.floor(r * 1000 + 0.5) / 1000
  return {
    snapshot_at: snapshotAt,
    window_days: DAYS,
    thresholds: { low_margin: LOW, high_margin: HIGH, slow_days: SLOW_DAYS },
    goods: {
      positions: goods.length,
      costed: costed.length,
      reserved: goods.filter((i) => i.available === 0).length,
      retail_all: sum(goods, (i) => i.retail_value ?? 0),
      retail,
      cost,
      profit: retail - cost,
      margin: retail ? (retail - cost) / retail : null,
      markup: cost ? (retail - cost) / cost : null,
      median_margin: median,
      sold: sold.length,
      revenue_90d: revenue,
      list_90d: atList,
      cogs_90d: cogs,
      discount_90d: atList ? 1 - revenue / atList : null,
      list_margin_90d: atList ? (atList - cogs) / atList : null,
      realized_margin_90d: revenue ? (revenue - cogs) / revenue : null,
      slow: slow.length,
      slow_profit: sum(slow, (i) => i.profit ?? 0),
      problem_items: goods.filter((i) => i.flags.some((f) => PROBLEM.includes(f))).length,
      problems: Object.fromEntries(PROBLEM.map((f) => [f, goods.filter((i) => i.flags.includes(f)).length])),
      buckets: BUCKETS.map(([from, to]) => ({
        from,
        to,
        count: margins.filter((m) => {
          const x = shown(m)
          return (from == null || x >= from) && (to == null || x < to)
        }).length,
      })),
    },
    excluded: { certificate: excluded(items, 'certificate'), promo: excluded(items, 'promo') },
    items,
  }
}
