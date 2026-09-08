/**
 * Which header filters a page actually applies.
 *
 * The filter bar is global chrome: every page gets period, sales type,
 * source, category, brand and promocode. Not every page's API accepts them.
 * `/api/traffic/*` takes `period`, `sales_type` and `source_id` and nothing
 * else — FastAPI drops the rest silently, so choosing a brand on /traffic
 * changed nothing at all and the page gave no sign of it.
 *
 * Two things read this map, and they have to agree: the bar hides a control
 * that would do nothing, and `useQueryParams` leaves the value out of the
 * request rather than sending something the server ignores. The value itself
 * stays in the store, so a brand chosen on /products is still chosen when the
 * reader goes back.
 *
 * Everything not listed keeps every filter — today's behaviour for every
 * other tab. This is deliberately not a survey of all eight: /traffic is the
 * one that was measured. Narrowing another page means checking its endpoints
 * first, and the mechanism is here for whoever does.
 */

export type FilterName =
  | 'period'
  | 'salesType'
  | 'sourceId'
  | 'categoryId'
  | 'brand'
  | 'promocode'

const ALL: readonly FilterName[] = [
  'period', 'salesType', 'sourceId', 'categoryId', 'brand', 'promocode',
]

// Keyed by pathname, both the /v2 and the bare form the router accepts.
const BY_PATH: Record<string, readonly FilterName[]> = {
  '/traffic': ['period', 'salesType', 'sourceId'],
  '/v2/traffic': ['period', 'salesType', 'sourceId'],
}

export function filtersForPath(path: string): ReadonlySet<FilterName> {
  return new Set(BY_PATH[path] ?? ALL)
}
