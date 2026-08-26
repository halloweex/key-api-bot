/**
 * The audience of an SMS campaign: how it is held on screen, and how it
 * travels to the server.
 *
 * One function builds the query string for all three uses — the live preview,
 * the CSV download and freezing the campaign — because they have to agree.
 * When the preview and the export were assembled separately, the roster that
 * got frozen was not always the one the manager had been looking at.
 *
 * Everything is optional. An empty audience is not "no customers": it is the
 * tier rules on their own, which is exactly what the page selected before
 * filters existed, so a campaign built the old way is still reproducible.
 */
import type {
  SmsAudienceCriteria,
  SmsAudienceFilters,
  SmsGrouping,
  SmsLtvBasis,
  SmsTier,
  SmsTierRules,
} from '../types/api'

export const DEFAULT_HOLDOUT_PCT = 10

// The base window, in days: the outermost rule, applied before every filter.
// It has a floor and a ceiling on the server (30..730); the form clamps to the
// same range so a typo is a narrower audience rather than a 422.
export const DEFAULT_WINDOW_DAYS = 270
export const MIN_WINDOW_DAYS = 30
export const MAX_WINDOW_DAYS = 730

export function emptyAudience(): SmsAudienceCriteria {
  return {
    // One arm by default. The tier cascade is not only a way of splitting an
    // audience — it drops anyone matching none of its three conditions, which
    // is most one-order buyers — and a default must not quietly remove people
    // nobody asked to remove.
    grouping: 'single',
    ltvBasis: 'margin',
    holdoutPct: DEFAULT_HOLDOUT_PCT,
    maxRecencyDays: DEFAULT_WINDOW_DAYS,
    tiers: [],
    tierRules: {},
    filters: {},
  }
}

/** Is anything actually filtered, or is this the plain tier cohort? */
export function hasFilters(filters: SmsAudienceFilters): boolean {
  return Object.values(filters).some((v) =>
    Array.isArray(v) ? v.length > 0 : v !== null && v !== undefined && v !== '',
  )
}

/** How many filter fields are set — the badge on the collapsed filter panel. */
export function countFilters(filters: SmsAudienceFilters): number {
  return Object.values(filters).filter((v) =>
    Array.isArray(v) ? v.length > 0 : v !== null && v !== undefined && v !== '',
  ).length
}

const NUMERIC_PARAMS: Array<[keyof SmsAudienceFilters, string]> = [
  ['recencyMin', 'recency_min'],
  ['recencyMax', 'recency_max'],
  ['ordersMin', 'orders_min'],
  ['ordersMax', 'orders_max'],
  ['ltvMin', 'ltv_min'],
  ['ltvMax', 'ltv_max'],
  ['aovMin', 'aov_min'],
  ['aovMax', 'aov_max'],
  ['boughtWithinDays', 'bought_within_days'],
]

const TEXT_PARAMS: Array<[keyof SmsAudienceFilters, string]> = [
  ['firstOrderFrom', 'first_order_from'],
  ['firstOrderTo', 'first_order_to'],
  ['promocodeUsed', 'promocode_used'],
]

const LIST_PARAMS: Array<[keyof SmsAudienceFilters, string]> = [
  ['cities', 'city'],
  ['brands', 'brand'],
  ['categoryIds', 'category_id'],
  ['sourceIds', 'source_id'],
]

/**
 * Serialise the audience as query parameters.
 *
 * Flat parameters rather than a JSON body on purpose: the CSV download is a
 * link the browser follows, and the API's session gate reads `sales_type`
 * from the query string.
 */
export function audienceToParams(
  audience: SmsAudienceCriteria,
  extra: Record<string, string | undefined> = {},
): string {
  const p = new URLSearchParams({
    ltv_basis: audience.ltvBasis,
    holdout_pct: String(audience.holdoutPct),
    grouping: audience.grouping,
    max_recency_days: String(audience.maxRecencyDays),
  })

  // The value level is a filter — "send to VIP only" — and it applies whether
  // or not the result is measured in arms. Its cut-offs travel with it, since
  // they are what defines the levels being filtered on.
  if (audience.tiers.length > 0) p.set('tier', audience.tiers.join(','))
  {
    const rules: Array<[keyof SmsTierRules, string]> = [
      ['vipLtv', 'vip_ltv'],
      ['coreLtv', 'core_ltv'],
      ['coreMinOrders', 'core_min_orders'],
      ['reactivationMaxRecency', 'reactivation_max_recency'],
    ]
    for (const [key, param] of rules) {
      const value = audience.tierRules[key]
      if (typeof value === 'number' && Number.isFinite(value)) {
        p.set(param, String(value))
      }
    }
  }

  const f = audience.filters
  for (const [key, param] of NUMERIC_PARAMS) {
    const value = f[key]
    if (typeof value === 'number' && Number.isFinite(value)) {
      p.set(param, String(value))
    }
  }
  for (const [key, param] of TEXT_PARAMS) {
    const value = f[key]
    if (typeof value === 'string' && value.trim()) p.set(param, value.trim())
  }
  for (const [key, param] of LIST_PARAMS) {
    const value = f[key]
    if (Array.isArray(value) && value.length > 0) p.set(param, value.join(','))
  }

  for (const [key, value] of Object.entries(extra)) {
    if (value !== undefined && value !== '') p.set(key, value)
  }
  return p.toString()
}

const GROUPINGS: SmsGrouping[] = ['rfm', 'single']
const BASES: SmsLtvBasis[] = ['revenue', 'margin']
const TIERS: SmsTier[] = ['VIP', 'CORE', 'REACTIVATION']

function num(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isFinite(value) ? value : undefined
}

function strings(value: unknown): string[] | undefined {
  if (!Array.isArray(value)) return undefined
  const out = value.filter((v): v is string => typeof v === 'string' && v.trim() !== '')
  return out.length ? out : undefined
}

function numbers(value: unknown): number[] | undefined {
  if (!Array.isArray(value)) return undefined
  const out = value.filter((v): v is number => typeof v === 'number')
  return out.length ? out : undefined
}

/**
 * Read a saved preset back into the form.
 *
 * Deliberately forgiving: a preset is stored as whatever the page put in it,
 * and one written by an older version of this page must still open. Anything
 * unrecognised is dropped rather than throwing — a preset that cannot be
 * opened is worse than one that opens with a field missing.
 */
export function audienceFromPreset(raw: unknown): SmsAudienceCriteria {
  const base = emptyAudience()
  if (!raw || typeof raw !== 'object') return base

  const src = raw as Record<string, unknown>
  const filtersSrc = (src.filters && typeof src.filters === 'object'
    ? src.filters
    : {}) as Record<string, unknown>

  const filters: SmsAudienceFilters = {}
  const numericKeys: Array<keyof SmsAudienceFilters> = [
    'recencyMin', 'recencyMax', 'ordersMin', 'ordersMax',
    'ltvMin', 'ltvMax', 'aovMin', 'aovMax', 'boughtWithinDays',
  ]
  for (const key of numericKeys) {
    const v = num(filtersSrc[key])
    if (v !== undefined) (filters[key] as number) = v
  }
  for (const key of ['firstOrderFrom', 'firstOrderTo', 'promocodeUsed'] as const) {
    const v = filtersSrc[key]
    if (typeof v === 'string' && v.trim()) filters[key] = v.trim()
  }
  const cities = strings(filtersSrc.cities)
  if (cities) filters.cities = cities
  const brands = strings(filtersSrc.brands)
  if (brands) filters.brands = brands
  const categoryIds = numbers(filtersSrc.categoryIds)
  if (categoryIds) filters.categoryIds = categoryIds
  const sourceIds = numbers(filtersSrc.sourceIds)
  if (sourceIds) filters.sourceIds = sourceIds

  const tiers = strings(src.tiers)?.filter((t): t is SmsTier =>
    (TIERS as string[]).includes(t)) ?? []

  const rulesSrc = (src.tierRules && typeof src.tierRules === 'object'
    ? src.tierRules
    : {}) as Record<string, unknown>
  const tierRules: SmsTierRules = {}
  for (const key of
    ['vipLtv', 'coreLtv', 'coreMinOrders', 'reactivationMaxRecency'] as const) {
    const v = num(rulesSrc[key])
    if (v !== undefined) tierRules[key] = v
  }

  return {
    grouping: GROUPINGS.includes(src.grouping as SmsGrouping)
      ? (src.grouping as SmsGrouping)
      : base.grouping,
    ltvBasis: BASES.includes(src.ltvBasis as SmsLtvBasis)
      ? (src.ltvBasis as SmsLtvBasis)
      : base.ltvBasis,
    holdoutPct: num(src.holdoutPct) ?? base.holdoutPct,
    maxRecencyDays: num(src.maxRecencyDays) ?? base.maxRecencyDays,
    tiers,
    tierRules,
    filters,
  }
}

/**
 * The audience in one line: how it is split, how it ranks, what is withheld,
 * how far back it looks, and how many filters narrow it.
 *
 * It exists because picking a saved audience with no filters changes nothing
 * visible, which reads as a broken control rather than as an audience that
 * genuinely has no filters. Two of the audiences that ship with the page are
 * exactly that.
 */
export function describeAudience(
  audience: SmsAudienceCriteria,
  t: (key: string, opts?: Record<string, unknown>) => string,
): string {
  const n = countFilters(audience.filters)
  return [
    t(`sms.grouping.${audience.grouping}`),
    t(audience.ltvBasis === 'margin' ? 'sms.basisMargin' : 'sms.basisRevenue'),
    t('sms.summaryHoldout', { pct: audience.holdoutPct }),
    t('sms.summaryWindow', { days: audience.maxRecencyDays }),
    n === 0 ? t('sms.summaryNoFilters') : t('sms.summaryFilters', { count: n }),
  ].join(' · ')
}

/**
 * Ranges whose edges are the wrong way round.
 *
 * Typing "90" into the lower box and starting on the upper one puts the filter
 * through `90..2` on the way to `90..237`. The server refuses that — rightly,
 * it selects nobody — but refusing it mid-keystroke turns typing a number into
 * an error message. So the page holds the request instead, and says which pair
 * is inverted.
 */
export function invertedRanges(filters: SmsAudienceFilters): string[] {
  const pairs: Array<[string, unknown, unknown]> = [
    ['recency', filters.recencyMin, filters.recencyMax],
    ['orders', filters.ordersMin, filters.ordersMax],
    ['ltv', filters.ltvMin, filters.ltvMax],
    ['aov', filters.aovMin, filters.aovMax],
    ['firstOrder', filters.firstOrderFrom, filters.firstOrderTo],
  ]
  return pairs
    .filter(([, lo, hi]) =>
      lo != null && hi != null && lo !== '' && hi !== '' && (lo as never) > (hi as never))
    .map(([name]) => name)
}

/**
 * The recency window, said in dates instead of days.
 *
 * A manager plans a campaign in dates — "bought this year, quiet since May" —
 * and had to convert that into "90 to 237 days", where 237 is the number of
 * days since 1 January and is wrong again tomorrow.
 *
 * Days stay the stored form on purpose: a saved audience of "quiet 90+ days"
 * still means that next month, while a saved date freezes into an ever
 * narrower group. So the dates are a view over the days, not a second filter.
 *
 * Recency runs backwards — the *earliest* last order is the *largest* number
 * of days — so `lastOrderFrom` pairs with `recencyMax` and `lastOrderTo` with
 * `recencyMin`.
 */
function startOfDay(d: Date): number {
  return new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime()
}

export function daysAgoToDate(days: number | null | undefined): string {
  if (typeof days !== 'number' || !Number.isFinite(days)) return ''
  const d = new Date()
  d.setDate(d.getDate() - Math.round(days))
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
}

export function dateToDaysAgo(iso: string): number | undefined {
  if (!iso) return undefined
  const [y, m, d] = iso.split('-').map(Number)
  if (!y || !m || !d) return undefined
  const days = Math.round(
    (startOfDay(new Date()) - startOfDay(new Date(y, m - 1, d))) / 86_400_000,
  )
  // A date in the future is not a recency; treat it as today.
  return Math.max(0, days)
}

/**
 * The frozen audience, said in phrases.
 *
 * The stored snapshot uses the store's own field names, not the form's, and it
 * is deliberately read loosely: a campaign frozen by an older version of the
 * page must still describe itself rather than render blank.
 */
export function describeFrozenCriteria(
  criteria: Record<string, unknown> | undefined,
  t: (key: string, opts?: Record<string, unknown>) => string,
): string[] {
  if (!criteria) return []
  const out: string[] = []
  const f = (criteria.filters ?? {}) as Record<string, unknown>
  const val = (k: string) => f[k]
  const has = (k: string) => val(k) !== undefined && val(k) !== null && val(k) !== ''

  const grouping = criteria.grouping === 'rfm' ? 'rfm' : 'single'
  out.push(t(`sms.grouping.${grouping}`))
  if (criteria.ltvBasis) {
    out.push(t(criteria.ltvBasis === 'margin' ? 'sms.basisMargin' : 'sms.basisRevenue'))
  }
  if (typeof criteria.holdoutPct === 'number') {
    out.push(t('sms.summaryHoldout', { pct: criteria.holdoutPct }))
  }
  if (typeof criteria.maxRecencyDays === 'number') {
    out.push(t('sms.summaryWindow', { days: criteria.maxRecencyDays }))
  }

  const pairs: Array<[string, string, string]> = [
    ['recency_min_days', 'recency_max_days', 'sms.filterRecency'],
    ['orders_min', 'orders_max', 'sms.filterOrders'],
    ['ltv_min', 'ltv_max', 'sms.filterLtv'],
    ['aov_min', 'aov_max', 'sms.filterAov'],
    ['first_order_from', 'first_order_to', 'sms.filterFirstOrder'],
  ]
  for (const [lo, hi, label] of pairs) {
    if (!has(lo) && !has(hi)) continue
    out.push(`${t(label)}: ${has(lo) ? val(lo) : '…'} — ${has(hi) ? val(hi) : '…'}`)
  }

  const lists: Array<[string, string]> = [
    ['brands', 'sms.filterBrand'],
    ['cities', 'sms.filterCity'],
    ['category_ids', 'sms.filterCategory'],
    ['source_ids', 'sms.filterSource'],
  ]
  for (const [key, label] of lists) {
    const v = val(key)
    if (Array.isArray(v) && v.length) out.push(`${t(label)}: ${v.join(', ')}`)
  }
  if (has('promocode')) out.push(`${t('sms.filterPromocode')}: ${val('promocode')}`)
  if (has('bought_within_days')) {
    out.push(`${t('sms.filterBoughtWithin')}: ${val('bought_within_days')}`)
  }
  return out
}
