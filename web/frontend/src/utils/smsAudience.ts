/**
 * The audience of an SMS campaign in the server's terms, and how it travels.
 *
 * The page builds an audience out of conditions (`smsConditions.ts`) and
 * converts it to this shape — a window and filters per group — only to ask a
 * question. This module is that last step and nothing else.
 *
 * One function builds the query string for all three uses — the live preview,
 * the CSV download and freezing the campaign — because they have to agree.
 * When the preview and the export were assembled separately, the roster that
 * got frozen was not always the one the manager had been looking at.
 *
 * An audience is one or more GROUPS, ORed: a customer gets the message if
 * they match any of them. The first group travels as the flat parameters it
 * always travelled as, so an audience of one group sends exactly the query
 * string it sent before groups existed. Any further group rides in one
 * parameter, `or_groups`, as JSON keyed by those same parameter names.
 *
 * Everything is optional. An empty group is not "no customers": it is the
 * tier rules on their own inside its window, which is exactly what the page
 * selected before filters existed, so a campaign built the old way is still
 * reproducible.
 */
import type {
  SmsAudienceCriteria,
  SmsAudienceFilters,
  SmsAudienceGroup,
  SmsGrouping,
  SmsLtvBasis,
  SmsTier,
  SmsTierRules,
} from '../types/api'

export const DEFAULT_HOLDOUT_PCT = 10

// A group's window, in days: the outermost rule of that group, applied before
// its filters. It has a floor and a ceiling on the server (30..3650). The page
// no longer shows it: it is derived from the "last order" condition, and a
// group without one sends the ceiling, which is all time — ten years covers
// the whole history with room. 270 is what a preset from before conditions
// meant when it said nothing.
export const DEFAULT_WINDOW_DAYS = 270
export const MIN_WINDOW_DAYS = 30
export const MAX_WINDOW_DAYS = 3650
export const ALL_TIME_DAYS = MAX_WINDOW_DAYS

// How many groups one audience may OR together — the server's
// MAX_AUDIENCE_GROUPS, refused there with a 400 past it.
export const MAX_AUDIENCE_GROUPS = 5

/** A group with nothing set: everyone inside the default window. */
export function emptyGroup(): SmsAudienceGroup {
  return { maxRecencyDays: DEFAULT_WINDOW_DAYS, filters: {} }
}

export function emptyAudience(): SmsAudienceCriteria {
  return {
    // One arm by default. The tier cascade is not only a way of splitting an
    // audience — it drops anyone matching none of its three conditions, which
    // is most one-order buyers — and a default must not quietly remove people
    // nobody asked to remove.
    grouping: 'single',
    ltvBasis: 'margin',
    holdoutPct: DEFAULT_HOLDOUT_PCT,
    tiers: [],
    tierRules: {},
    groups: [emptyGroup()],
  }
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
  ['periodOrdersMin', 'period_orders_min'],
  ['periodOrdersMax', 'period_orders_max'],
  ['periodDays', 'period_days'],
]

const TEXT_PARAMS: Array<[keyof SmsAudienceFilters, string]> = [
  ['firstOrderFrom', 'first_order_from'],
  ['firstOrderTo', 'first_order_to'],
  ['promocodeUsed', 'promocode_used'],
  ['genderMinConfidence', 'gender_min_confidence'],
  ['periodFrom', 'period_from'],
  ['periodTo', 'period_to'],
]

const LIST_PARAMS: Array<[keyof SmsAudienceFilters, string]> = [
  ['genders', 'gender'],
  ['cities', 'city'],
  ['brands', 'brand'],
  ['categoryIds', 'category_id'],
  ['sourceIds', 'source_id'],
]

/** Every filter in a group that is set, under its API name, as JSON values. */
function groupValues(group: SmsAudienceGroup): Record<string, number | string | Array<number | string>> {
  const out: Record<string, number | string | Array<number | string>> = {
    max_recency_days: group.maxRecencyDays,
  }
  const f = group.filters
  for (const [key, param] of NUMERIC_PARAMS) {
    const value = f[key]
    if (typeof value === 'number' && Number.isFinite(value)) out[param] = value
  }
  for (const [key, param] of TEXT_PARAMS) {
    const value = f[key]
    if (typeof value === 'string' && value.trim()) out[param] = value.trim()
  }
  for (const [key, param] of LIST_PARAMS) {
    const value = f[key]
    if (Array.isArray(value) && value.length > 0) out[param] = [...value]
  }
  return out
}

/**
 * Serialise the audience as query parameters.
 *
 * Flat parameters rather than a JSON body on purpose: the CSV download is a
 * link the browser follows, and the API's session gate reads `sales_type`
 * from the query string. The first group is flat, as it always was; the rest
 * are one JSON array in `or_groups`, whose keys are the flat names.
 */
export function audienceToParams(
  audience: SmsAudienceCriteria,
  extra: Record<string, string | undefined> = {},
): string {
  const [first, ...rest] = audience.groups.length ? audience.groups : [emptyGroup()]
  const p = new URLSearchParams({
    ltv_basis: audience.ltvBasis,
    holdout_pct: String(audience.holdoutPct),
    grouping: audience.grouping,
    max_recency_days: String(first.maxRecencyDays),
  })

  // The value level is a filter — "send to VIP only" — and it applies whether
  // or not the result is measured in arms. Its cut-offs travel with it, since
  // they are what defines the levels being filtered on. Common to the whole
  // campaign, not to a group.
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

  const values = groupValues(first)
  for (const [param, value] of Object.entries(values)) {
    if (param === 'max_recency_days') continue
    p.set(param, Array.isArray(value) ? value.join(',') : String(value))
  }
  if (rest.length > 0) {
    p.set('or_groups', JSON.stringify(rest.map(groupValues)))
  }

  for (const [key, value] of Object.entries(extra)) {
    if (value !== undefined && value !== '') p.set(key, value)
  }
  return p.toString()
}

/**
 * The audience as a preset stores it.
 *
 * `groups` holds the whole audience. The first group is ALSO written where a
 * preset kept it before groups existed — `maxRecencyDays` and `filters` at the
 * top — so a page still running the previous version, which reads only those,
 * opens it as its first group rather than as an empty audience of everyone.
 */
export function audienceToPreset(audience: SmsAudienceCriteria): Record<string, unknown> {
  const first = audience.groups[0] ?? emptyGroup()
  return {
    ...audience,
    maxRecencyDays: first.maxRecencyDays,
    filters: first.filters,
  }
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

/** One group out of whatever a preset or draft stored for it. */
function groupFromRaw(raw: unknown): SmsAudienceGroup {
  const src = (raw && typeof raw === 'object' ? raw : {}) as Record<string, unknown>
  const filtersSrc = (src.filters && typeof src.filters === 'object'
    ? src.filters
    : {}) as Record<string, unknown>

  const filters: SmsAudienceFilters = {}
  const numericKeys: Array<keyof SmsAudienceFilters> = [
    'recencyMin', 'recencyMax', 'ordersMin', 'ordersMax',
    'ltvMin', 'ltvMax', 'aovMin', 'aovMax', 'boughtWithinDays',
    'periodOrdersMin', 'periodOrdersMax', 'periodDays',
  ]
  for (const key of numericKeys) {
    const v = num(filtersSrc[key])
    if (v !== undefined) (filters[key] as number) = v
  }
  for (const key of
    ['firstOrderFrom', 'firstOrderTo', 'promocodeUsed', 'periodFrom', 'periodTo'] as const) {
    const v = filtersSrc[key]
    if (typeof v === 'string' && v.trim()) filters[key] = v.trim()
  }
  // A period is dates or the last N days, never both — the server refuses
  // the pair. A preset holding both keeps the dates, the more specific.
  if ((filters.periodFrom || filters.periodTo) && filters.periodDays != null) {
    delete filters.periodDays
  }
  const cities = strings(filtersSrc.cities)
  if (cities) filters.cities = cities
  // Unknown values are dropped rather than carried: a preset is form state
  // somebody saved, and a stored 'x' would reach the API as a 400 on a screen
  // the person only opened to look at.
  const genders = strings(filtersSrc.genders)
    ?.filter((g): g is 'f' | 'm' => g === 'f' || g === 'm')
  if (genders?.length) filters.genders = genders
  const conf = filtersSrc.genderMinConfidence
  if (conf === 'certain' || conf === 'high' || conf === 'medium') {
    filters.genderMinConfidence = conf
  }
  const brands = strings(filtersSrc.brands)
  if (brands) filters.brands = brands
  const categoryIds = numbers(filtersSrc.categoryIds)
  if (categoryIds) filters.categoryIds = categoryIds
  const sourceIds = numbers(filtersSrc.sourceIds)
  if (sourceIds) filters.sourceIds = sourceIds

  return {
    maxRecencyDays: num(src.maxRecencyDays) ?? DEFAULT_WINDOW_DAYS,
    filters,
  }
}

/**
 * Read a saved preset back into the form.
 *
 * Deliberately forgiving: a preset is stored as whatever the page put in it,
 * and one written by an older version of this page must still open. Anything
 * unrecognised is dropped rather than throwing — a preset that cannot be
 * opened is worse than one that opens with a field missing.
 *
 * A preset from before groups has its one group at the top level
 * (`maxRecencyDays`, `filters`); one from after has `groups`.
 */
export function audienceFromPreset(raw: unknown): SmsAudienceCriteria {
  const base = emptyAudience()
  if (!raw || typeof raw !== 'object') return base

  const src = raw as Record<string, unknown>

  const groups = Array.isArray(src.groups) && src.groups.length > 0
    ? src.groups.slice(0, MAX_AUDIENCE_GROUPS).map(groupFromRaw)
    : [groupFromRaw({ maxRecencyDays: src.maxRecencyDays, filters: src.filters })]

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
    tiers,
    tierRules,
    groups,
  }
}
