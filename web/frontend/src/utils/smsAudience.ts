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
} from '../types/api'

export const DEFAULT_HOLDOUT_PCT = 10

export function emptyAudience(): SmsAudienceCriteria {
  return {
    grouping: 'rfm',
    ltvBasis: 'margin',
    holdoutPct: DEFAULT_HOLDOUT_PCT,
    tiers: [],
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
  })

  // Tier subsets only mean something when there are tiers. Sending them with
  // grouping=single would read as a filter that silently does nothing.
  if (audience.grouping === 'rfm' && audience.tiers.length > 0) {
    p.set('tier', audience.tiers.join(','))
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

  return {
    grouping: GROUPINGS.includes(src.grouping as SmsGrouping)
      ? (src.grouping as SmsGrouping)
      : base.grouping,
    ltvBasis: BASES.includes(src.ltvBasis as SmsLtvBasis)
      ? (src.ltvBasis as SmsLtvBasis)
      : base.ltvBasis,
    holdoutPct: num(src.holdoutPct) ?? base.holdoutPct,
    tiers,
    filters,
  }
}
