import type { FunnelCounts, FunnelRow, FunnelTrafficType, FunnelWindow, TrafficFunnelResponse } from '../types/api'

// Illustrative funnels for the tests and stories — not production's numbers,
// but built to land where a real month does: the conversion within noise,
// one step clearly down, a year ago far lower with warm traffic grown.
// Every breakdown fits inside the site's own counts, and the types are the
// sum of their channels, as `web/services/ga4_funnel_service.assemble` builds
// them.

export const c = (e: number, v: number, a: number, b: number, p: number): FunnelCounts => ({
  entered: e, viewed_product: v, added_to_cart: a, began_checkout: b, purchased: p,
})

const row = (
  key: string, now: FunnelCounts, prev: FunnelCounts, ago: FunnelCounts, type?: FunnelTrafficType,
): FunnelRow => ({ key, type, current: now, previous: prev, year_ago: ago })

/** This period: 2.03 % bought. */
export const NOW = c(34000, 18700, 2420, 1015, 690)
/** The period before: 2.33 %, within noise of NOW; cart → checkout 47.7 % against 41.9 %. */
export const PREVIOUS = c(30000, 16500, 2200, 1050, 700)
/** A year ago: 1.13 %, product → cart 8.4 %. */
export const YEAR_AGO = c(60000, 29000, 2450, 1100, 680)
/** As NOW within noise, every step too. */
export const STEADY = c(33000, 18150, 2350, 990, 668)

export const BY_CHANNEL: FunnelRow[] = [
  row('Organic Social', c(9600, 4600, 830, 300, 205), c(7300, 3300, 650, 290, 190), c(11000, 4800, 700, 300, 190), 'warm'),
  row('Paid Social', c(8700, 5800, 460, 180, 120), c(8200, 5200, 380, 150, 110), c(28000, 13000, 400, 170, 100), 'cold'),
  row('Direct', c(5900, 3000, 320, 170, 120), c(3900, 2300, 300, 165, 108), c(4000, 2100, 440, 230, 145), 'warm'),
  row('Cross-network', c(4300, 2800, 360, 170, 115), c(4300, 2600, 360, 180, 105), c(4000, 2000, 200, 80, 50), 'cold'),
  row('Organic Search', c(3630, 1810, 325, 128, 91), c(4450, 2320, 410, 205, 149), c(4600, 2400, 330, 156, 95), 'warm'),
  row('Unassigned', c(1400, 560, 70, 38, 20), c(1300, 500, 55, 33, 20), c(5200, 3000, 230, 90, 55), 'other'),
  row('Email', c(170, 90, 25, 12, 9), c(150, 80, 20, 10, 8), c(400, 200, 30, 14, 10), 'warm'),
  row('Referral', c(100, 40, 25, 12, 8), c(200, 150, 20, 12, 8), c(2600, 1300, 100, 50, 30), 'other'),
]

function sumOf(rows: FunnelRow[], w: FunnelWindow): FunnelCounts {
  return rows.reduce(
    (s, r) => c(
      s.entered + r[w].entered, s.viewed_product + r[w].viewed_product, s.added_to_cart + r[w].added_to_cart,
      s.began_checkout + r[w].began_checkout, s.purchased + r[w].purchased,
    ),
    c(0, 0, 0, 0, 0),
  )
}

export const BY_TYPE: FunnelRow[] = (['cold', 'warm', 'other'] as const).map((type) => {
  const rows = BY_CHANNEL.filter((r) => r.type === type)
  return { key: type, current: sumOf(rows, 'current'), previous: sumOf(rows, 'previous'), year_ago: sumOf(rows, 'year_ago') }
})

export const BY_DEVICE: FunnelRow[] = [
  row('mobile', c(28600, 15650, 1950, 800, 545), c(25300, 13900, 1800, 860, 575), c(50000, 24000, 1950, 860, 520)),
  row('desktop', c(4800, 2700, 450, 205, 140), c(4200, 2300, 380, 180, 120), c(9000, 4500, 470, 225, 150)),
  row('smart tv', c(280, 250, 0, 0, 0), c(0, 0, 0, 0, 0), c(0, 0, 0, 0, 0)),
  row('tablet', c(160, 90, 12, 6, 3), c(150, 80, 10, 5, 3), c(500, 300, 20, 10, 6)),
]

export const FUNNEL: TrafficFunnelResponse = {
  periods: {
    current: { start: '2026-10-01', end: '2026-10-31' },
    previous: { start: '2026-08-31', end: '2026-09-30' },
    year_ago: { start: '2025-10-01', end: '2025-10-31' },
  },
  earliest: '2025-09-04',
  steps: ['entered', 'viewed_product', 'added_to_cart', 'began_checkout', 'purchased'],
  total: { current: NOW, previous: PREVIOUS, year_ago: YEAR_AGO },
  by_type: BY_TYPE,
  by_channel: BY_CHANNEL,
  by_device: BY_DEVICE,
  country: 'ua',
  stale: false,
  fetched_at: '2026-11-04T08:42:00Z',
}
