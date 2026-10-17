/**
 * An SMS audience as the person building it thinks of it: groups of
 * conditions, each condition one sentence.
 *
 * Until 2026-10 the page held the audience in the server's own terms — a base
 * window per group and fifteen filter fields — and showed every one of them.
 * The owner's first real campaign through it typed 92 days into an order
 * count, put the period's end date into its only date field, mixed up the
 * lifetime count with the count in a period, and lost January to a window
 * nobody had set. Each of those was a field doing something nobody could see.
 *
 * So the page holds CONDITIONS now, and converts them to the server's
 * parameters only when it asks a question (`definitionToCriteria`):
 *
 * - one condition is one sentence: what, how often, when;
 * - lifetime orders and orders in a period are one condition with a period
 *   control, and "all time" is a value of that control;
 * - the base window is not on screen. It is derived from the "last order"
 *   condition, and without one it is all time.
 *
 * Absolute dates stay dates and relative stays relative: "the last 90 days" is
 * kept as 90 and resolved against today each time it is sent, "1 Jan – 14 Oct"
 * is kept as two dates. The old form stored "last order between dates" as
 * days, which quietly moved the dates every morning.
 *
 * The transport is unchanged: `definitionToCriteria` produces the
 * `SmsAudienceCriteria` that `audienceToParams` always serialised, so the
 * preview, the CSV and the freeze still ask one question.
 */
import type {
  SmsAudienceCriteria, SmsAudienceFilters, SmsAudienceGroup, SmsGrouping,
  SmsLtvBasis, SmsTier, SmsTierRules,
} from '../types/api'
import {
  ALL_TIME_DAYS, audienceFromPreset, audienceToPreset, DEFAULT_HOLDOUT_PCT,
  MAX_AUDIENCE_GROUPS, MIN_WINDOW_DAYS,
} from './smsAudience'
import { addDays, daysAgo, daysBetween, isIsoDate, kyivToday } from './smsDates'

// ─── Shapes ──────────────────────────────────────────────────────────────────

/** When an order count is counted: ever, the last N days, or between dates. */
export type PeriodValue =
  | { kind: 'all' }
  | { kind: 'last'; days: number | null }
  /** Either end may be '' — "since 1 Jan" or "up to 14 Oct" — not both. */
  | { kind: 'range'; from: string; to: string }

export type CountOp = 'atLeast' | 'exactly' | 'atMost' | 'between' | 'none'

/** When an order happened, for the first and the last one. */
export type WhenValue =
  | { kind: 'within'; days: number | null }
  /** N or more days ago. */
  | { kind: 'before'; days: number | null }
  /** Between N and M days ago — "quiet for three to six months". */
  | { kind: 'ago'; min: number | null; max: number | null }
  | { kind: 'dates'; from: string; to: string }

interface Base { id: string }

export interface OrdersCondition extends Base {
  type: 'orders'
  op: CountOp
  /** The count, or the lower end for `between`. Unused for `none`. */
  count: number | null
  /** The upper end, for `between` only. */
  countMax: number | null
  period: PeriodValue
}
export interface LastOrderCondition extends Base { type: 'lastOrder'; when: WhenValue }
export interface FirstOrderCondition extends Base { type: 'firstOrder'; when: WhenValue }
export interface MoneyCondition extends Base {
  type: 'spent' | 'aov'
  min: number | null
  max: number | null
}
/**
 * What they bought. One condition, never split: the server asks brand,
 * category and promo code of ONE order line (one EXISTS), so three separate
 * rows would promise a meaning the query does not have.
 */
export interface BoughtCondition extends Base {
  type: 'bought'
  brands: string[]
  categoryIds: number[]
  promocode: string
  /** null is all time. */
  withinDays: number | null
}
export interface SourceCondition extends Base { type: 'source'; sourceIds: number[] }
export interface CityCondition extends Base { type: 'city'; cities: string[] }
export interface GenderCondition extends Base {
  type: 'gender'
  genders: Array<'f' | 'm'>
  confidence: 'certain' | 'high' | 'medium' | null
}

export type Condition =
  | OrdersCondition | LastOrderCondition | FirstOrderCondition | MoneyCondition
  | BoughtCondition | SourceCondition | CityCondition | GenderCondition

export type ConditionType = Condition['type']

/** All of its conditions must hold. An empty group is every buyer. */
export interface ConditionGroup { id: string; conditions: Condition[] }

/**
 * The whole audience. `groups` decides who; everything else is the campaign's
 * and applies to the union — the value level as a filter, and how the result
 * is measured (step 2).
 */
export interface AudienceDefinition {
  grouping: SmsGrouping
  ltvBasis: SmsLtvBasis
  holdoutPct: number
  tiers: SmsTier[]
  tierRules: SmsTierRules
  groups: ConditionGroup[]
}

// The order the "+ Condition" menu lists them in, by family.
export const CONDITION_FAMILIES: Array<{ key: string; types: ConditionType[] }> = [
  { key: 'history', types: ['orders', 'lastOrder', 'firstOrder', 'spent', 'aov'] },
  { key: 'bought', types: ['bought', 'source'] },
  { key: 'who', types: ['city', 'gender'] },
]

// The active sources, as Silver classifies them. Opencart (3) is retired and
// never reaches revenue, so offering it would offer an empty set. Labels are
// `sms.source.<id>`: "Shopify" is the website to the people using this page.
export const SOURCES: number[] = [1, 2, 4, 5]

// Bounds the server holds; a value outside them is a 422, so the page treats
// it as not filled in rather than sending it.
export const MAX_DAYS = 3650
export const MAX_ORDERS = 1000
/** Above this an order count is almost certainly days typed into the wrong
 *  box — 92 was, on 2026-10-17. */
export const SUSPICIOUS_ORDER_COUNT = 20
export const DAY_CHIPS = [30, 60, 90, 180, 365]

let seq = 0
export function newId(prefix = 'c'): string {
  seq += 1
  return `${prefix}${Date.now().toString(36)}${seq.toString(36)}`
}

// ─── Building ────────────────────────────────────────────────────────────────

/** Orders conditions in a group are at most two: one over all time and one
 *  over a period. The server has one lifetime count and one period per group. */
function ordersKinds(conditions: Condition[], except?: string) {
  const others = conditions.filter(
    (c): c is OrdersCondition => c.type === 'orders' && c.id !== except,
  )
  return {
    lifetime: others.some((c) => c.period.kind === 'all'),
    period: others.some((c) => c.period.kind !== 'all'),
  }
}

/** Which period kinds an orders condition may take beside its siblings. */
export function allowedPeriodKinds(
  condition: OrdersCondition, siblings: Condition[],
): Array<PeriodValue['kind']> {
  const taken = ordersKinds(siblings, condition.id)
  return taken.lifetime ? ['last', 'range'] : taken.period ? ['all'] : ['all', 'last', 'range']
}

/** Can another condition of this type go into this group? */
export function canAdd(type: ConditionType, conditions: Condition[]): boolean {
  if (type === 'orders') {
    const taken = ordersKinds(conditions)
    return !(taken.lifetime && taken.period)
  }
  return !conditions.some((c) => c.type === type)
}

/** A condition as it is inserted: with defaults, ready to edit. */
export function newCondition(type: ConditionType, siblings: Condition[] = []): Condition {
  const id = newId()
  switch (type) {
    case 'orders': {
      // The second orders condition takes whichever kind the first left free.
      const lifetimeTaken = ordersKinds(siblings).lifetime
      return lifetimeTaken
        ? { id, type, op: 'atLeast', count: 1, countMax: null,
            period: { kind: 'last', days: 90 } }
        : { id, type, op: 'atLeast', count: 2, countMax: null, period: { kind: 'all' } }
    }
    case 'lastOrder':
      return { id, type, when: { kind: 'within', days: 90 } }
    case 'firstOrder':
      return { id, type, when: { kind: 'within', days: 30 } }
    case 'spent':
    case 'aov':
      return { id, type, min: null, max: null }
    case 'bought':
      return { id, type, brands: [], categoryIds: [], promocode: '', withinDays: null }
    case 'source':
      return { id, type, sourceIds: [] }
    case 'city':
      return { id, type, cities: [] }
    case 'gender':
      return { id, type, genders: [], confidence: null }
  }
}

export function emptyConditionGroup(): ConditionGroup {
  return { id: newId('g'), conditions: [] }
}

export function emptyDefinition(): AudienceDefinition {
  return {
    // One arm by default. The tier cascade drops anyone matching none of its
    // three rules — most one-order buyers — and a default must not remove
    // people nobody asked to remove.
    grouping: 'single',
    ltvBasis: 'margin',
    holdoutPct: DEFAULT_HOLDOUT_PCT,
    tiers: [],
    tierRules: {},
    groups: [emptyConditionGroup()],
  }
}

// ─── Is it filled in? ────────────────────────────────────────────────────────

export type ConditionProblem = 'incomplete' | 'inverted'

const isDays = (n: number | null | undefined, min = 1): n is number =>
  typeof n === 'number' && Number.isInteger(n) && n >= min && n <= MAX_DAYS
const isCount = (n: number | null | undefined): n is number =>
  typeof n === 'number' && Number.isInteger(n) && n >= 1 && n <= MAX_ORDERS
const isMoney = (n: number | null | undefined): n is number =>
  typeof n === 'number' && Number.isFinite(n)

function periodProblem(p: PeriodValue): ConditionProblem | null {
  if (p.kind === 'all') return null
  if (p.kind === 'last') return isDays(p.days) ? null : 'incomplete'
  const from = isIsoDate(p.from) ? p.from : ''
  const to = isIsoDate(p.to) ? p.to : ''
  if (!from && !to) return 'incomplete'
  if (from && to && from > to) return 'inverted'
  return null
}

function whenProblem(w: WhenValue): ConditionProblem | null {
  switch (w.kind) {
    case 'within':
    case 'before':
      return isDays(w.days) ? null : 'incomplete'
    case 'ago': {
      const lo = isDays(w.min, 0) ? w.min : null
      const hi = isDays(w.max, 0) ? w.max : null
      if (lo == null && hi == null) return 'incomplete'
      if (lo != null && hi != null && lo > hi) return 'inverted'
      return null
    }
    case 'dates': {
      const from = isIsoDate(w.from) ? w.from : ''
      const to = isIsoDate(w.to) ? w.to : ''
      if (!from && !to) return 'incomplete'
      if (from && to && from > to) return 'inverted'
      return null
    }
  }
}

/**
 * Why a condition cannot be sent yet, or null when it can.
 *
 * An unfinished condition is left out of the preview — the row says so — and
 * holds the step: a campaign must never be frozen with a condition the person
 * believes is applied and is not.
 */
export function conditionProblem(c: Condition): ConditionProblem | null {
  switch (c.type) {
    case 'orders': {
      if (c.op === 'none') return c.period.kind === 'all' ? 'incomplete' : periodProblem(c.period)
      if (!isCount(c.count)) return 'incomplete'
      if (c.op === 'between') {
        if (!isCount(c.countMax)) return 'incomplete'
        if (c.count > c.countMax) return 'inverted'
      }
      return periodProblem(c.period)
    }
    case 'lastOrder':
    case 'firstOrder':
      return whenProblem(c.when)
    case 'spent':
    case 'aov': {
      const lo = isMoney(c.min) ? c.min : null
      const hi = isMoney(c.max) ? c.max : null
      if (lo == null && hi == null) return 'incomplete'
      if (lo != null && hi != null && lo > hi) return 'inverted'
      return null
    }
    case 'bought':
      if (!c.brands.length && !c.categoryIds.length && !c.promocode.trim()) return 'incomplete'
      if (c.withinDays != null && !isDays(c.withinDays)) return 'incomplete'
      return null
    case 'source':
      return c.sourceIds.length ? null : 'incomplete'
    case 'city':
      return c.cities.length ? null : 'incomplete'
    case 'gender':
      // "Everybody whose gender we are sure about" is sayable on its own.
      return c.genders.length || c.confidence ? null : 'incomplete'
  }
}

/** Every unfinished condition, by group (1-based) — what holds the step. */
export function unfinished(
  def: AudienceDefinition,
): Array<{ group: number; id: string; problem: ConditionProblem }> {
  const out: Array<{ group: number; id: string; problem: ConditionProblem }> = []
  def.groups.forEach((g, i) => {
    for (const c of g.conditions) {
      const problem = conditionProblem(c)
      if (problem) out.push({ group: i + 1, id: c.id, problem })
    }
  })
  return out
}

/** Is this order count probably a number of days typed into the wrong box? */
export function suspiciousCount(c: Condition): boolean {
  return c.type === 'orders' && c.op !== 'none'
    && typeof c.count === 'number' && c.count > SUSPICIOUS_ORDER_COUNT
}

/** The one-tap fix for `suspiciousCount`: the number becomes the period. */
export function moveCountIntoPeriod(c: OrdersCondition): OrdersCondition {
  const days = Math.min(MAX_DAYS, c.count ?? 0)
  return { ...c, op: 'atLeast', count: 1, countMax: null, period: { kind: 'last', days } }
}

// ─── To the server ───────────────────────────────────────────────────────────

/** [lower, upper] on an order count; undefined is "no bound". */
function countBounds(c: OrdersCondition): [number | undefined, number | undefined] {
  switch (c.op) {
    case 'atLeast': return [c.count!, undefined]
    case 'exactly': return [c.count!, c.count!]
    case 'atMost': return [undefined, c.count!]
    case 'between': return [c.count!, c.countMax!]
    case 'none': return [undefined, 0]
  }
}

/** [min, max] days since the order, resolved against `today`. */
function recencyBounds(w: WhenValue, today: string): [number | undefined, number | undefined] {
  switch (w.kind) {
    case 'within': return [undefined, w.days!]
    case 'before': return [w.days!, undefined]
    case 'ago': return [w.min ?? undefined, w.max ?? undefined]
    case 'dates':
      // The later date is the fewer days: last order between F and T is
      // recency between today−T and today−F. Capped at the server's bound,
      // which a date ten years back would otherwise pass (a 422).
      return [
        isIsoDate(w.to) ? Math.min(MAX_DAYS, daysAgo(w.to, today)) : undefined,
        isIsoDate(w.from) ? Math.min(MAX_DAYS, daysAgo(w.from, today)) : undefined,
      ]
  }
}

/** [from, to] dates of the first order, resolved against `today`. */
function firstOrderDates(w: WhenValue, today: string): [string | undefined, string | undefined] {
  switch (w.kind) {
    case 'within': return [addDays(today, -w.days!), undefined]
    case 'before': return [undefined, addDays(today, -w.days!)]
    case 'ago':
      return [
        w.max != null ? addDays(today, -w.max) : undefined,
        w.min != null ? addDays(today, -w.min) : undefined,
      ]
    case 'dates':
      return [isIsoDate(w.from) ? w.from : undefined, isIsoDate(w.to) ? w.to : undefined]
  }
}

/**
 * One group in the server's terms: its window and its filters.
 *
 * The window is derived, never typed. With a "last order" condition it is that
 * condition's upper bound — the shape the audience always had, so "last order
 * in the last 270 days" sends exactly `max_recency_days=270` and the built-in
 * presets ask what they always asked. Without one it is all time: there is no
 * hidden window left to cut a period that starts in January.
 *
 * Unfinished conditions are skipped; the row says so and the step is held.
 */
export function groupToCriteria(group: ConditionGroup, today = kyivToday()): SmsAudienceGroup {
  const f: SmsAudienceFilters = {}
  let recencyHi: number | undefined

  for (const c of group.conditions) {
    if (conditionProblem(c)) continue
    switch (c.type) {
      case 'orders': {
        const [lo, hi] = countBounds(c)
        if (c.period.kind === 'all') {
          if (lo !== undefined) f.ordersMin = lo
          if (hi !== undefined) f.ordersMax = hi
          break
        }
        // One period per group on the server. The page never builds two
        // (`allowedPeriodKinds`, `canAdd`), and a second one here would be a
        // 400 or, worse, silently replace the first.
        if (f.periodDays != null || f.periodFrom || f.periodTo) break
        if (lo !== undefined) f.periodOrdersMin = lo
        if (hi !== undefined) f.periodOrdersMax = hi
        if (c.period.kind === 'last') {
          f.periodDays = c.period.days!
        } else {
          if (isIsoDate(c.period.from)) f.periodFrom = c.period.from
          if (isIsoDate(c.period.to)) f.periodTo = c.period.to
        }
        break
      }
      case 'lastOrder': {
        const [lo, hi] = recencyBounds(c.when, today)
        // "0 or more days ago" is everybody; sending it would only change the
        // shape of the query.
        if (lo !== undefined && lo > 0) f.recencyMin = lo
        if (hi !== undefined) recencyHi = hi
        break
      }
      case 'firstOrder': {
        const [from, to] = firstOrderDates(c.when, today)
        if (from) f.firstOrderFrom = from
        if (to) f.firstOrderTo = to
        break
      }
      case 'spent':
        if (isMoney(c.min)) f.ltvMin = c.min
        if (isMoney(c.max)) f.ltvMax = c.max
        break
      case 'aov':
        if (isMoney(c.min)) f.aovMin = c.min
        if (isMoney(c.max)) f.aovMax = c.max
        break
      case 'bought':
        if (c.brands.length) f.brands = [...c.brands]
        if (c.categoryIds.length) f.categoryIds = [...c.categoryIds]
        if (c.promocode.trim()) f.promocodeUsed = c.promocode.trim()
        if (c.withinDays != null) f.boughtWithinDays = c.withinDays
        break
      case 'source':
        f.sourceIds = [...c.sourceIds]
        break
      case 'city':
        f.cities = [...c.cities]
        break
      case 'gender':
        if (c.genders.length) f.genders = [...c.genders]
        if (c.confidence) f.genderMinConfidence = c.confidence
        break
    }
  }

  let window = ALL_TIME_DAYS
  if (recencyHi !== undefined) {
    if (recencyHi >= MIN_WINDOW_DAYS) {
      window = Math.min(recencyHi, ALL_TIME_DAYS)
    } else {
      // The server's window starts at 30 days; anything narrower is the
      // window's floor plus the condition itself.
      window = MIN_WINDOW_DAYS
      f.recencyMax = recencyHi
    }
  }
  return { maxRecencyDays: window, filters: f }
}

// What the server fills in when a threshold is left blank, by basis.
const TIER_DEFAULTS: Record<SmsLtvBasis, { vipLtv: number; coreLtv: number }> = {
  margin: { vipLtv: 5500, coreLtv: 2750 },
  revenue: { vipLtv: 10000, coreLtv: 5000 },
}

/**
 * Typed thresholds the server would refuse, by name.
 *
 * Typing "6000" into the VIP cut-off passes through "6", which is below the
 * Core default, and the server answers 400 — on a page that then shows nothing
 * but the error. So a threshold that cannot stand is left out of the query and
 * said so beside its box, the way an unfinished condition is.
 */
export function tierRuleProblems(def: AudienceDefinition): Array<keyof SmsTierRules> {
  const r = def.tierRules
  const out: Array<keyof SmsTierRules> = []
  const money = (v: number | null | undefined) => typeof v === 'number' && v >= 0 && v <= 10_000_000
  if (r.vipLtv != null && !money(r.vipLtv)) out.push('vipLtv')
  if (r.coreLtv != null && !money(r.coreLtv)) out.push('coreLtv')
  const vip = money(r.vipLtv) ? r.vipLtv! : TIER_DEFAULTS[def.ltvBasis].vipLtv
  const core = money(r.coreLtv) ? r.coreLtv! : TIER_DEFAULTS[def.ltvBasis].coreLtv
  if (core > vip) out.push(r.coreLtv != null ? 'coreLtv' : 'vipLtv')
  if (r.coreMinOrders != null && !(Number.isInteger(r.coreMinOrders)
      && r.coreMinOrders >= 2 && r.coreMinOrders <= 50)) out.push('coreMinOrders')
  if (r.reactivationMaxRecency != null && !(Number.isInteger(r.reactivationMaxRecency)
      && r.reactivationMaxRecency >= 7 && r.reactivationMaxRecency <= 730)) {
    out.push('reactivationMaxRecency')
  }
  return [...new Set(out)]
}

/**
 * Groups with nothing in them, 1-based, when there is more than one group.
 *
 * An empty group is every buyer, and every buyer OR anything is every buyer —
 * which is what a freshly added second group would quietly make the audience.
 * The card says so; the step is held until it has a condition or is removed.
 */
export function emptyExtraGroups(def: AudienceDefinition): number[] {
  if (def.groups.length < 2) return []
  return def.groups.map((g, i) => (g.conditions.length ? 0 : i + 1)).filter(Boolean)
}

export function definitionToCriteria(
  def: AudienceDefinition, today = kyivToday(),
): SmsAudienceCriteria {
  const groups = (def.groups.length ? def.groups : [emptyConditionGroup()])
    .map((g) => groupToCriteria(g, today))
  const tierRules = { ...def.tierRules }
  for (const key of tierRuleProblems(def)) delete tierRules[key]
  // The reactivation level lives inside the base window, and the server
  // refuses a cut-off past it. The window is derived now, so nobody can see
  // why a typed 120 would be refused under "last order in the last 60 days";
  // the cut-off simply cannot reach further than the window does.
  const widest = Math.max(...groups.map((g) => g.maxRecencyDays))
  if (typeof tierRules.reactivationMaxRecency === 'number'
      && tierRules.reactivationMaxRecency > widest) {
    tierRules.reactivationMaxRecency = widest
  }
  return {
    grouping: def.grouping,
    ltvBasis: def.ltvBasis,
    holdoutPct: def.holdoutPct,
    tiers: [...def.tiers],
    tierRules,
    groups,
  }
}

// ─── From what was stored ────────────────────────────────────────────────────

function num(v: unknown): number | null {
  return typeof v === 'number' && Number.isFinite(v) ? v : null
}

function strList(v: unknown): string[] {
  return Array.isArray(v)
    ? v.filter((x): x is string => typeof x === 'string' && x.trim() !== '')
    : []
}

function numList(v: unknown): number[] {
  return Array.isArray(v)
    ? v.filter((x): x is number => typeof x === 'number' && Number.isFinite(x))
    : []
}

function dateOr(v: unknown): string {
  return isIsoDate(v) ? v : ''
}

type Counted = Pick<OrdersCondition, 'op' | 'count' | 'countMax'>

function counted(min: number | null, max: number | null): Counted | null {
  // "At least 0 orders" bounds nothing — every customer here has ordered —
  // and read as a count it would be an unfinished condition holding the step.
  if (min != null && min <= 0) min = null
  if (min == null && max == null) return null
  if (max === 0) return { op: 'none', count: null, countMax: null }
  if (min != null && max != null) {
    return min === max
      ? { op: 'exactly', count: min, countMax: null }
      : { op: 'between', count: min, countMax: max }
  }
  if (min != null) return { op: 'atLeast', count: min, countMax: null }
  return { op: 'atMost', count: max, countMax: null }
}

/**
 * One group of the server's terms, said as conditions.
 *
 * The mapping the spec fixed: a window other than all time is "last order in
 * the last N days" (merged with any recency bounds into one condition), the
 * lifetime count is "orders over all time", the period count is "orders in
 * the period", and every other filter is its own condition.
 */
export function groupFromCriteria(group: SmsAudienceGroup): ConditionGroup {
  const f = group.filters ?? {}
  const conditions: Condition[] = []
  const id = () => newId()

  // Last order: the window and the recency bounds are one fact.
  const windowHi = group.maxRecencyDays < ALL_TIME_DAYS ? group.maxRecencyDays : undefined
  const hiCandidates = [windowHi, num(f.recencyMax) ?? undefined]
    .filter((v): v is number => v !== undefined)
  const hi = hiCandidates.length ? Math.min(...hiCandidates) : undefined
  const lo = num(f.recencyMin) ?? undefined
  if (lo !== undefined && hi !== undefined) {
    conditions.push({ id: id(), type: 'lastOrder', when: { kind: 'ago', min: lo, max: hi } })
  } else if (hi !== undefined) {
    conditions.push({ id: id(), type: 'lastOrder', when: { kind: 'within', days: hi } })
  } else if (lo !== undefined) {
    conditions.push({ id: id(), type: 'lastOrder', when: { kind: 'before', days: lo } })
  }

  const lifetime = counted(num(f.ordersMin), num(f.ordersMax))
  if (lifetime) conditions.push({ id: id(), type: 'orders', ...lifetime, period: { kind: 'all' } })

  const periodDays = num(f.periodDays)
  const periodFrom = dateOr(f.periodFrom)
  const periodTo = dateOr(f.periodTo)
  if (periodDays != null || periodFrom || periodTo) {
    // A period with no count means at least one — the server's rule.
    const inPeriod = counted(num(f.periodOrdersMin), num(f.periodOrdersMax))
      ?? { op: 'atLeast' as const, count: 1, countMax: null }
    conditions.push({
      id: id(), type: 'orders', ...inPeriod,
      period: periodDays != null && !periodFrom && !periodTo
        ? { kind: 'last', days: periodDays }
        : { kind: 'range', from: periodFrom, to: periodTo },
    })
  }

  const firstFrom = dateOr(f.firstOrderFrom)
  const firstTo = dateOr(f.firstOrderTo)
  if (firstFrom || firstTo) {
    conditions.push({
      id: id(), type: 'firstOrder', when: { kind: 'dates', from: firstFrom, to: firstTo },
    })
  }

  if (num(f.ltvMin) != null || num(f.ltvMax) != null) {
    conditions.push({ id: id(), type: 'spent', min: num(f.ltvMin), max: num(f.ltvMax) })
  }
  if (num(f.aovMin) != null || num(f.aovMax) != null) {
    conditions.push({ id: id(), type: 'aov', min: num(f.aovMin), max: num(f.aovMax) })
  }

  const brands = strList(f.brands)
  const categoryIds = numList(f.categoryIds)
  const promocode = typeof f.promocodeUsed === 'string' ? f.promocodeUsed.trim() : ''
  // "Bought within N days" qualifies a purchase; on its own the server reads
  // nothing into it, so it is not carried without one.
  if (brands.length || categoryIds.length || promocode) {
    conditions.push({
      id: id(), type: 'bought', brands, categoryIds, promocode,
      withinDays: num(f.boughtWithinDays),
    })
  }

  const sourceIds = numList(f.sourceIds)
  if (sourceIds.length) conditions.push({ id: id(), type: 'source', sourceIds })
  const cities = strList(f.cities)
  if (cities.length) conditions.push({ id: id(), type: 'city', cities })
  const genders = strList(f.genders).filter((g): g is 'f' | 'm' => g === 'f' || g === 'm')
  const conf = f.genderMinConfidence
  const confidence = conf === 'certain' || conf === 'high' || conf === 'medium' ? conf : null
  if (genders.length || confidence) {
    conditions.push({ id: id(), type: 'gender', genders, confidence })
  }

  return { id: newId('g'), conditions }
}

export function definitionFromCriteria(c: SmsAudienceCriteria): AudienceDefinition {
  return {
    grouping: c.grouping,
    ltvBasis: c.ltvBasis,
    holdoutPct: c.holdoutPct,
    tiers: [...c.tiers],
    tierRules: { ...c.tierRules },
    groups: (c.groups.length ? c.groups : [{ maxRecencyDays: ALL_TIME_DAYS, filters: {} }])
      .slice(0, MAX_AUDIENCE_GROUPS)
      .map(groupFromCriteria),
  }
}

function periodFromRaw(raw: unknown): PeriodValue {
  const p = (raw && typeof raw === 'object' ? raw : {}) as Record<string, unknown>
  if (p.kind === 'last') return { kind: 'last', days: num(p.days) }
  if (p.kind === 'range') return { kind: 'range', from: dateOr(p.from), to: dateOr(p.to) }
  return { kind: 'all' }
}

function whenFromRaw(raw: unknown): WhenValue {
  const w = (raw && typeof raw === 'object' ? raw : {}) as Record<string, unknown>
  switch (w.kind) {
    case 'before': return { kind: 'before', days: num(w.days) }
    case 'ago': return { kind: 'ago', min: num(w.min), max: num(w.max) }
    case 'dates': return { kind: 'dates', from: dateOr(w.from), to: dateOr(w.to) }
    default: return { kind: 'within', days: num(w.days) }
  }
}

const OPS: CountOp[] = ['atLeast', 'exactly', 'atMost', 'between', 'none']

/** One stored condition, or null for anything this page does not know. */
function conditionFromRaw(raw: unknown): Condition | null {
  if (!raw || typeof raw !== 'object') return null
  const c = raw as Record<string, unknown>
  const id = newId()
  switch (c.type) {
    case 'orders':
      return {
        id, type: 'orders',
        op: OPS.includes(c.op as CountOp) ? (c.op as CountOp) : 'atLeast',
        count: num(c.count), countMax: num(c.countMax), period: periodFromRaw(c.period),
      }
    case 'lastOrder':
    case 'firstOrder':
      return { id, type: c.type, when: whenFromRaw(c.when) }
    case 'spent':
    case 'aov':
      return { id, type: c.type, min: num(c.min), max: num(c.max) }
    case 'bought':
      return {
        id, type: 'bought', brands: strList(c.brands), categoryIds: numList(c.categoryIds),
        promocode: typeof c.promocode === 'string' ? c.promocode : '',
        withinDays: num(c.withinDays),
      }
    case 'source':
      return { id, type: 'source', sourceIds: numList(c.sourceIds) }
    case 'city':
      return { id, type: 'city', cities: strList(c.cities) }
    case 'gender': {
      const conf = c.confidence
      return {
        id, type: 'gender',
        genders: strList(c.genders).filter((g): g is 'f' | 'm' => g === 'f' || g === 'm'),
        confidence: conf === 'certain' || conf === 'high' || conf === 'medium' ? conf : null,
      }
    }
    default:
      return null
  }
}

/**
 * A preset or a draft, back into conditions.
 *
 * A preset saved by this page carries `groups[].conditions` beside the
 * server-shaped `maxRecencyDays`/`filters`; those win, because they keep dates
 * as dates. One saved before conditions existed has only the server shape,
 * and is mapped by `groupFromCriteria`. Forgiving throughout: a preset that
 * cannot be opened is worse than one that opens with a field missing.
 */
export function definitionFromPreset(raw: unknown): AudienceDefinition {
  const base = definitionFromCriteria(audienceFromPreset(raw))
  if (!raw || typeof raw !== 'object') return base
  const src = raw as Record<string, unknown>
  if (!Array.isArray(src.groups) || !src.groups.length) return base
  const stored = src.groups as unknown[]
  const anyConditions = stored.some(
    (g) => g && typeof g === 'object' && Array.isArray((g as Record<string, unknown>).conditions),
  )
  if (!anyConditions) return base
  return {
    ...base,
    groups: stored.slice(0, MAX_AUDIENCE_GROUPS).map((g, i) => {
      const conditions = (g as Record<string, unknown> | null)?.conditions
      return Array.isArray(conditions)
        ? {
            id: newId('g'),
            conditions: conditions.map(conditionFromRaw).filter((c): c is Condition => c !== null),
          }
        : base.groups[i] ?? emptyConditionGroup()
    }),
  }
}

/** Conditions as a preset stores them — without the ids, which are the page's. */
function storedConditions(group: ConditionGroup): Array<Record<string, unknown>> {
  return group.conditions.map((c) => {
    const rest: Record<string, unknown> = { ...c }
    delete rest.id
    return rest
  })
}

/**
 * The audience as a preset or a draft stores it.
 *
 * The server shape is kept beside the conditions — group 1 also at the top
 * level — so a page still running the previous version opens it as the same
 * audience rather than as everybody.
 */
export function definitionToPreset(
  def: AudienceDefinition, today = kyivToday(),
): Record<string, unknown> {
  const criteria = definitionToCriteria(def, today)
  const stored = audienceToPreset(criteria)
  return {
    ...stored,
    groups: criteria.groups.map((g, i) => ({
      ...g, conditions: storedConditions(def.groups[i] ?? emptyConditionGroup()),
    })),
  }
}

// The store's names for the filters, as a frozen campaign records them.
const STORE_TO_FORM: Array<[string, keyof SmsAudienceFilters]> = [
  ['recency_min_days', 'recencyMin'], ['recency_max_days', 'recencyMax'],
  ['orders_min', 'ordersMin'], ['orders_max', 'ordersMax'],
  ['ltv_min', 'ltvMin'], ['ltv_max', 'ltvMax'], ['aov_min', 'aovMin'], ['aov_max', 'aovMax'],
  ['first_order_from', 'firstOrderFrom'], ['first_order_to', 'firstOrderTo'],
  ['cities', 'cities'], ['genders', 'genders'],
  ['gender_min_confidence', 'genderMinConfidence'],
  ['brands', 'brands'], ['category_ids', 'categoryIds'], ['source_ids', 'sourceIds'],
  ['promocode', 'promocodeUsed'], ['bought_within_days', 'boughtWithinDays'],
  ['period_orders_min', 'periodOrdersMin'], ['period_orders_max', 'periodOrdersMax'],
  ['period_from', 'periodFrom'], ['period_to', 'periodTo'], ['period_days', 'periodDays'],
]

function filtersFromStore(raw: unknown): SmsAudienceFilters {
  const src = (raw && typeof raw === 'object' ? raw : {}) as Record<string, unknown>
  const out: Record<string, unknown> = {}
  for (const [store, form] of STORE_TO_FORM) {
    if (src[store] !== undefined && src[store] !== null) out[form] = src[store]
  }
  return out as SmsAudienceFilters
}

/**
 * A frozen campaign's definition, to build the next one like it.
 *
 * It loads how the people were CHOSEN (`criteria`), not the roster: the next
 * campaign "like the last one" is the same question asked today.
 */
export function definitionFromFrozen(
  criteria: Record<string, unknown> | undefined,
): AudienceDefinition {
  const base = emptyDefinition()
  if (!criteria) return base
  const groupsRaw = Array.isArray(criteria.groups) && criteria.groups.length
    ? criteria.groups
    : [{ maxRecencyDays: criteria.maxRecencyDays, filters: criteria.filters }]
  const groups = groupsRaw.slice(0, MAX_AUDIENCE_GROUPS).map((raw) => {
    const g = (raw && typeof raw === 'object' ? raw : {}) as Record<string, unknown>
    return groupFromCriteria({
      maxRecencyDays: num(g.maxRecencyDays) ?? ALL_TIME_DAYS,
      filters: filtersFromStore(g.filters),
    })
  })
  const ltvBasis: SmsLtvBasis = criteria.ltvBasis === 'revenue' ? 'revenue' : 'margin'
  // The resolved thresholds are recorded; only one that differs from the
  // basis default was a decision worth carrying.
  const tierRules: SmsTierRules = {}
  const vip = num(criteria.vipLtv)
  const core = num(criteria.coreLtv)
  if (vip != null && vip !== TIER_DEFAULTS[ltvBasis].vipLtv) tierRules.vipLtv = vip
  if (core != null && core !== TIER_DEFAULTS[ltvBasis].coreLtv) tierRules.coreLtv = core
  const coreOrders = num(criteria.coreMinOrders)
  if (coreOrders != null && coreOrders !== 2) tierRules.coreMinOrders = coreOrders
  const reactivation = num(criteria.reactivationMaxRecency)
  if (reactivation != null && reactivation !== 120) tierRules.reactivationMaxRecency = reactivation
  // The value level, frozen since 2026-10-17; a campaign frozen before that
  // recorded none, and loads as every level.
  const tiers = strList(criteria.tiers)
    .map((t) => t.toUpperCase())
    .filter((t): t is SmsTier => t === 'VIP' || t === 'CORE' || t === 'REACTIVATION')
  return {
    ...base,
    grouping: criteria.grouping === 'rfm' ? 'rfm' : 'single',
    ltvBasis,
    holdoutPct: num(criteria.holdoutPct) ?? base.holdoutPct,
    tiers,
    tierRules,
    groups,
  }
}

// ─── Dates that age ──────────────────────────────────────────────────────────

/** Does this audience name calendar dates (which a later campaign may want moved)? */
export function hasAbsoluteDates(def: AudienceDefinition): boolean {
  return def.groups.some((g) => g.conditions.some((c) =>
    (c.type === 'orders' && c.period.kind === 'range')
    || ((c.type === 'lastOrder' || c.type === 'firstOrder') && c.when.kind === 'dates')))
}

/**
 * Every date in the audience moved by `days` — "the same campaign as on
 * 1 October, but today" is 1 Jan – 14 Oct becoming 17 Jan – 30 Oct.
 */
export function shiftDates(def: AudienceDefinition, days: number): AudienceDefinition {
  const move = (iso: string) => (isIsoDate(iso) ? addDays(iso, days) : iso)
  return {
    ...def,
    groups: def.groups.map((g) => ({
      ...g,
      conditions: g.conditions.map((c): Condition => {
        if (c.type === 'orders' && c.period.kind === 'range') {
          return {
            ...c, period: { kind: 'range', from: move(c.period.from), to: move(c.period.to) },
          }
        }
        if ((c.type === 'lastOrder' || c.type === 'firstOrder') && c.when.kind === 'dates') {
          return { ...c, when: { kind: 'dates', from: move(c.when.from), to: move(c.when.to) } }
        }
        return c
      }),
    })),
  }
}

/** How far a campaign frozen at `frozenAt` (an ISO instant) lies behind today. */
export function daysSince(frozenAt: string | null | undefined, today = kyivToday()): number {
  if (!frozenAt) return 0
  const at = new Date(frozenAt)
  if (Number.isNaN(at.getTime())) return 0
  return Math.max(0, daysBetween(kyivToday(at), today))
}

// ─── Templates ───────────────────────────────────────────────────────────────

export type TemplateKey =
  | 'all' | 'recent' | 'new' | 'repeat' | 'oneAndGone' | 'churning' | 'lost'
  | 'vip' | 'brand'

export const TEMPLATE_KEYS: TemplateKey[] = [
  'all', 'recent', 'new', 'repeat', 'oneAndGone', 'churning', 'lost', 'vip', 'brand',
]

/**
 * A template's groups and level. Opened as ordinary, editable conditions —
 * a template is a starting point, never a black box. The campaign's own
 * settings (holdout, measurement) are kept: a template says who, not how.
 *
 * `focus` names the condition that has to be filled in before the template
 * means anything ("bought a brand…" opens on the brand).
 */
export function applyTemplate(
  key: TemplateKey, def: AudienceDefinition,
): { definition: AudienceDefinition; focus?: string } {
  const group = (...conditions: Condition[]): ConditionGroup => ({ id: newId('g'), conditions })
  const orders = (op: CountOp, count: number | null, period: PeriodValue): OrdersCondition =>
    ({ id: newId(), type: 'orders', op, count, countMax: null, period })
  const last = (when: WhenValue): LastOrderCondition => ({ id: newId(), type: 'lastOrder', when })
  const keep = (groups: ConditionGroup[], tiers: SmsTier[] = []) =>
    ({ definition: { ...def, tiers, groups } })

  switch (key) {
    case 'all': return keep([group(orders('atLeast', 1, { kind: 'all' }))])
    case 'recent': return keep([group(orders('atLeast', 1, { kind: 'last', days: 30 }))])
    case 'new':
      return keep([group({ id: newId(), type: 'firstOrder', when: { kind: 'within', days: 30 } })])
    case 'repeat': return keep([group(orders('atLeast', 2, { kind: 'all' }))])
    case 'oneAndGone':
      return keep([group(
        orders('exactly', 1, { kind: 'all' }), last({ kind: 'before', days: 60 }),
      )])
    case 'churning': return keep([group(last({ kind: 'ago', min: 90, max: 180 }))])
    case 'lost': return keep([group(last({ kind: 'before', days: 240 }))])
    case 'vip': return keep([emptyConditionGroup()], ['VIP'])
    case 'brand': {
      const bought = newCondition('bought')
      return { ...keep([group(bought)]), focus: bought.id }
    }
  }
}
