import { describe, it, expect } from 'vitest'
import {
  NO_FILTERS, inBucket, inView, money, narrowed, percent, sortItems, toCsv, unitPrice, visibleItems,
} from '../stockMargin'
import type { StockMarginBucket, StockMarginItem } from '../../types/api'

function item(offer_id: number, p: Partial<StockMarginItem> = {}): StockMarginItem {
  return {
    offer_id, product_id: offer_id * 10, sku: `SKU-${offer_id}`, name: `Item ${offer_id}`,
    brand: 'Brand', category: 'Creams', root_category: 'Face', kind: 'goods',
    quantity: 10, reserve: 0, available: 10, price: 600, cost: 300, costed: true,
    margin: 0.5, markup: 1, unit_profit: 300, retail_value: 6000, cost_value: 3000, profit: 3000,
    units_90d: 9, revenue_90d: 5000, avg_price_90d: 555.56, discount_90d: 0.074,
    realized_margin_90d: 0.46, moved_90d: 9, days_of_supply: 100, slow: false,
    last_sale_date: '2026-11-20', flags: [], ...p,
  }
}

const BUCKETS: StockMarginBucket[] = [
  { from: null, to: 0, count: 0 }, { from: 0, to: 0.2, count: 0 }, { from: 0.2, to: 0.3, count: 0 },
  { from: 0.3, to: 0.4, count: 0 }, { from: 0.4, to: 0.5, count: 0 }, { from: 0.5, to: 0.6, count: 0 },
  { from: 0.6, to: null, count: 0 },
]

describe('views', () => {
  const goods = item(1)
  const noCost = item(2, { cost: null, costed: false, margin: null, flags: ['no_cost'] })
  const slow = item(3, { slow: true, flags: ['slow'] })
  const cert = item(4, { kind: 'certificate', costed: false, margin: null })

  it('keeps certificates and testers out of the goods and in their own view', () => {
    expect([goods, noCost, slow, cert].filter((i) => inView(i, 'goods'))).toEqual([goods, noCost, slow])
    expect([goods, noCost, slow, cert].filter((i) => inView(i, 'other'))).toEqual([cert])
  })

  it('counts a problem only on goods, and a slow row is not a problem by itself', () => {
    expect([goods, noCost, slow, cert].filter((i) => inView(i, 'problems'))).toEqual([noCost])
    expect([goods, noCost, slow, cert].filter((i) => inView(i, 'slow'))).toEqual([slow])
    expect([goods, noCost, slow, cert].filter((i) => inView(i, 'nodata'))).toEqual([noCost])
  })
})

describe('buckets', () => {
  it('files a margin as it is shown, lower edge included', () => {
    expect(inBucket(0.4, BUCKETS[4])).toBe(true)          // 40.0% is "40–50%"
    expect(inBucket(0.4, BUCKETS[3])).toBe(false)
    expect(inBucket(0.49996, BUCKETS[5])).toBe(true)      // shown as 50.0%
    expect(inBucket(-0.2, BUCKETS[0])).toBe(true)
    expect(inBucket(0.6, BUCKETS[6])).toBe(true)
  })

  it('narrows the goods to the bucket picked on the chart', () => {
    const rows = [item(1, { margin: 0.4 }), item(2, { margin: 0.55 }), item(3, { margin: null, costed: false })]
    expect(visibleItems(rows, { ...NO_FILTERS, bucket: 4 }, BUCKETS).map((i) => i.offer_id)).toEqual([1])
  })
})

describe('brand and search', () => {
  it('match the name or the SKU, case aside, within the brand', () => {
    const rows = [item(1, { name: 'Matcha Cleanser' }), item(2, { brand: 'Other', name: 'Matcha Toner' }), item(3, { sku: 'ABC' })]
    expect(narrowed(rows, '', 'matcha').map((i) => i.offer_id)).toEqual([1, 2])
    expect(narrowed(rows, 'Brand', 'matcha').map((i) => i.offer_id)).toEqual([1])
    expect(narrowed(rows, '', 'abc').map((i) => i.offer_id)).toEqual([3])
  })
})

describe('sorting', () => {
  const rows = [
    item(1, { profit: 100 }),
    item(2, { profit: null, costed: false }),
    item(3, { profit: 300 }),
    item(4, { profit: 100 }),
  ]

  it('puts the rows without data last in either direction', () => {
    expect(sortItems(rows, 'profit', 'desc').map((i) => i.offer_id)).toEqual([3, 1, 4, 2])
    expect(sortItems(rows, 'profit', 'asc').map((i) => i.offer_id)).toEqual([1, 4, 3, 2])
  })

  it('reads no margin off a row that is not costed goods', () => {
    const cert = item(5, { kind: 'certificate', costed: false, margin: 0.96 })
    expect(sortItems([cert, item(6, { margin: 0.3 })], 'margin', 'desc').map((i) => i.offer_id)).toEqual([6, 5])
  })
})

describe('numbers', () => {
  it('puts an honest minus first and never prints a negative zero', () => {
    expect(money(-980)).toBe('−₴980')
    expect(percent(-0.172)).toBe('−17.2%')
    expect(percent(-0.0004)).toBe('0.0%')
    expect(percent(null)).toBe('—')
    expect(money(null)).toBe('—')
  })

  it('keeps the kopecks of a unit price only when it has them', () => {
    expect(unitPrice(600)).toBe('₴600')
    expect(unitPrice(7.5)).toMatch(/^₴7[.,]50$/)
  })
})

describe('csv', () => {
  it('quotes a cell with a comma, a quote or a line break', () => {
    expect(toCsv([['a', 'b,c', 'say "hi"', null, 3]])).toBe('a,"b,c","say ""hi""",,3')
  })
})
