import type { FunnelCounts, FunnelRow, FunnelStep, FunnelWindow, TrafficFunnelResponse } from '../types/api'

/**
 * The site funnel's arithmetic, kept apart from how it is drawn and how it is
 * said (`utils/funnelText`).
 *
 * A person is counted at a step once, and only after every step before it, so
 * each step is a share of the one before and of everyone who entered. Shares
 * are fractions here, 0..1; they become percentages only when shown.
 */

export const FUNNEL_STEPS: readonly FunnelStep[] = [
  'entered', 'viewed_product', 'added_to_cart', 'began_checkout', 'purchased',
]

/** A step somebody can fail to reach: every step but the first. */
export type Transition = Exclude<FunnelStep, 'entered'>

export const TRANSITIONS: readonly Transition[] = [
  'viewed_product', 'added_to_cart', 'began_checkout', 'purchased',
]

/** The windows a period is set beside. */
export type Against = Exclude<FunnelWindow, 'current'>

/** The step a transition is reached from. */
export function stepBefore(step: Transition): FunnelStep {
  return FUNNEL_STEPS[FUNNEL_STEPS.indexOf(step) - 1]
}

export function share(part: number, whole: number): number | null {
  return whole > 0 ? part / whole : null
}

export function hasPeople(counts: FunnelCounts | null | undefined): counts is FunnelCounts {
  return !!counts && counts.entered > 0
}

/** Of the people at the step before, the share who reached `step`. */
export function stepShare(counts: FunnelCounts, step: Transition): number | null {
  return share(counts[step], counts[stepBefore(step)])
}

/** Bought, out of everyone who entered: the funnel's total. */
export function conversion(counts: FunnelCounts): number | null {
  return share(counts.purchased, counts.entered)
}

// ─── Telling a change from noise ─────────────────────────────────────────────
//
// GA4's funnel moves more from day to day than chance alone would move it:
// over the daily funnels of September–October 2026 the spread was χ²/df ≈ 1.96
// of the binomial one. A plain two-proportion test called 10 of the last 12
// monthly changes in conversion notable, while the median monthly move was
// 0.34 pp — so the variance is widened by that factor before the usual 1.96
// cut. Measured, not assumed; re-measure if the site's traffic changes shape.

export const OVERDISPERSION = 1.96
const Z_CUT = 1.96

/** Below these the whole site claims nothing: one day is ~1 100 people and
 *  ~23 buyers, whose conversion swings by ±0.8 pp on its own. */
export const MIN_SITE_PEOPLE = 2000
export const MIN_SITE_BUYERS = 50
/** A row of the breakdown smaller than this is shown, never judged. */
export const MIN_GROUP_PEOPLE = 200
/** And no share is judged from fewer people than this, or with fewer
 *  expected on its rarer side. */
const MIN_BASE = 50
const MIN_EXPECTED = 10

export type Change = 'better' | 'worse' | 'same'

function valid(k: number, n: number): boolean {
  return Number.isFinite(k) && Number.isFinite(n) && n > 0 && k >= 0 && k <= n
}

/** The two-proportion z of k1/n1 against k2/n2 with GA4's noise widened in;
 *  null when there are too few people to judge. */
export function noiseZ(k1: number, n1: number, k2: number, n2: number): number | null {
  if (!valid(k1, n1) || !valid(k2, n2)) return null
  if (n1 < MIN_BASE || n2 < MIN_BASE) return null
  const p = (k1 + k2) / (n1 + n2)
  if (Math.min(n1, n2) * Math.min(p, 1 - p) < MIN_EXPECTED) return null
  const se = Math.sqrt(p * (1 - p) * (1 / n1 + 1 / n2) * OVERDISPERSION)
  return (k1 / n1 - k2 / n2) / se
}

export function judge(z: number | null): Change | null {
  if (z == null) return null
  return z >= Z_CUT ? 'better' : z <= -Z_CUT ? 'worse' : 'same'
}

/** Enough people on the whole site to say anything beyond the counts. */
export function enoughToClaim(counts: FunnelCounts | null | undefined): boolean {
  return !!counts && counts.entered >= MIN_SITE_PEOPLE && counts.purchased >= MIN_SITE_BUYERS
}

/** A step's share now against then: its z and what it says. */
export function stepChange(now: FunnelCounts, was: FunnelCounts, step: Transition): { z: number | null; change: Change | null } {
  const from = stepBefore(step)
  const z = noiseZ(now[step], now[from], was[step], was[from])
  return { z, change: judge(z) }
}

export function conversionChange(now: FunnelCounts, was: FunnelCounts): { z: number | null; change: Change | null } {
  const z = noiseZ(now.purchased, now.entered, was.purchased, was.entered)
  return { z, change: judge(z) }
}

// ─── What the period can be set beside ───────────────────────────────────────

/**
 * Whether a window can be compared with. GA4 keeps funnels 14 months, so a
 * window starting before `earliest` was asked for its last days only, and a
 * comparison would set a whole period beside part of one.
 */
export function canCompare(data: TrafficFunnelResponse, window: Against): boolean {
  return data.periods[window].start >= data.earliest && hasPeople(data.total[window])
}

// ─── The steps ───────────────────────────────────────────────────────────────

export interface StepFigure {
  step: FunnelStep
  people: number
  /** Of everyone who entered. */
  ofEntered: number | null
  /** Of the step before; null for the first. */
  ofBefore: number | null
  /** At the step before and not here; null for the first. */
  left: number | null
  /** The same step in the window compared with. */
  was: { people: number; ofBefore: number | null } | null
  /** `ofBefore` against the window's, beyond noise; null when not judged. */
  change: Change | null
}

export function funnelSteps(now: FunnelCounts, was: FunnelCounts | null, judged: boolean): StepFigure[] {
  return FUNNEL_STEPS.map((step) => {
    if (step === 'entered') {
      return {
        step, people: now.entered, ofEntered: share(now.entered, now.entered), ofBefore: null, left: null,
        was: was ? { people: was.entered, ofBefore: null } : null, change: null,
      }
    }
    const from = stepBefore(step)
    return {
      step,
      people: now[step],
      ofEntered: share(now[step], now.entered),
      ofBefore: stepShare(now, step),
      left: now[from] - now[step],
      was: was ? { people: was[step], ofBefore: stepShare(was, step) } : null,
      change: was && judged ? stepChange(now, was, step).change : null,
    }
  })
}

/** The step the most people fail to reach; the earlier one on a tie. */
export function biggestLeak(counts: FunnelCounts): Transition | null {
  let best: Transition | null = null
  let most = 0
  for (const step of TRANSITIONS) {
    const left = counts[stepBefore(step)] - counts[step]
    if (left > most) {
      most = left
      best = step
    }
  }
  return best
}

/**
 * Buyers a step's change made or cost, had every other step stayed as it is
 * now: its share then against now, times the people who stood before it now,
 * times the share of those who passed it now and went on to buy. October 2026
 * against September: cart → checkout 47.2 % → 42.2 % of 2 513 carts, 67 % of
 * those reaching checkout buying, is about 84 buyers.
 */
export function buyersMoved(now: FunnelCounts, was: FunnelCounts, step: Transition): number | null {
  const from = stepBefore(step)
  const rNow = share(now[step], now[from])
  const rWas = share(was[step], was[from])
  if (rNow == null || rWas == null) return null
  const onward = share(now.purchased, now[step]) ?? share(was.purchased, was[step])
  if (onward == null) return null
  return (rNow - rWas) * now[from] * onward
}

/** A count rounded the way it is said: «около 80», «около 250», «около 1 200». */
export function roughly(n: number): number {
  const a = Math.abs(n)
  if (a < 10) return Math.round(a)
  const digits = a < 100 ? 1 : 2
  const unit = 10 ** (Math.floor(Math.log10(a)) - digits + 1)
  return Math.round(a / unit) * unit
}

/** A rate as "N of every base": 2.02 % is 20 of every 1 000. The smallest base
 *  that gives at least ten. */
export function perBase(rate: number): { base: number; count: number } {
  for (const base of [100, 1000, 10000]) {
    if (rate * base >= 10) return { base, count: Math.round(rate * base) }
  }
  return { base: 10000, count: Math.round(rate * 10000) }
}

// ─── Counts ──────────────────────────────────────────────────────────────────

export type CountMove = 'more' | 'fewer' | 'same'

/** Within 5 % either way is "about the same". */
export const SAME_BAND = 0.05

export interface CountChange {
  move: CountMove
  /** now / then − 1; null when there was nobody then. */
  ratio: number | null
}

export function countChange(now: number, was: number): CountChange {
  if (was <= 0) return { move: now > 0 ? 'more' : 'same', ratio: null }
  const ratio = now / was - 1
  return { move: Math.abs(ratio) < SAME_BAND ? 'same' : ratio > 0 ? 'more' : 'fewer', ratio }
}

// ─── The traffic mix ─────────────────────────────────────────────────────────

export interface MixShift {
  /** Which way the conversion went, and the mix with it. */
  change: 'better' | 'worse'
  type: 'cold' | 'warm'
  /** That type's share of visitors grew. */
  more: boolean
}

/**
 * Whether the traffic mix — more warm, fewer cold — carried part of the
 * conversion's change, rather than each kind converting differently.
 *
 * The split is the symmetric one: a share's move is priced at the average of
 * the two windows' rates, so the parts add up whichever window comes first.
 * Over the rows' own sum, which is not the site's total (the breakdown's
 * footnote says why). Named only when the mix carried at least a fifth of
 * the change and pushed the same way, and only as cold or warm: October 2026
 * against October 2025 rose 0.92 pp, 0.20 of it from warm traffic growing from
 * 34 % to 56 % of visitors. September to October it was +0.02 of −0.30, and
 * nothing is said.
 */
export function mixShift(rows: FunnelRow[], window: Against): MixShift | null {
  const sum = (w: FunnelWindow, step: FunnelStep) => rows.reduce((s, r) => s + r[w][step], 0)
  const peopleNow = sum('current', 'entered')
  const peopleWas = sum(window, 'entered')
  if (peopleNow <= 0 || peopleWas <= 0) return null
  const total = sum('current', 'purchased') / peopleNow - sum(window, 'purchased') / peopleWas
  const parts = rows.map((r) => {
    const sNow = r.current.entered / peopleNow
    const sWas = r[window].entered / peopleWas
    const rNow = conversion(r.current)
    const rWas = conversion(r[window])
    const rate = rNow != null && rWas != null ? (rNow + rWas) / 2 : rNow ?? rWas ?? 0
    return { key: r.key, dShare: sNow - sWas, avgShare: (sNow + sWas) / 2, rate }
  })
  const level = parts.reduce((s, p) => s + p.avgShare * p.rate, 0)
  const pushes = parts.map((p) => ({ ...p, push: p.dShare * (p.rate - level) }))
  const mix = pushes.reduce((s, p) => s + p.push, 0)
  if (total === 0 || Math.sign(mix) !== Math.sign(total) || Math.abs(mix) < 0.2 * Math.abs(total)) return null
  const named = pushes
    .filter((p) => (p.key === 'cold' || p.key === 'warm') && Math.sign(p.push) === Math.sign(mix))
    .sort((a, b) => Math.abs(b.push) - Math.abs(a.push))[0]
  if (!named) return null
  return { change: total > 0 ? 'better' : 'worse', type: named.key as 'cold' | 'warm', more: named.dShare > 0 }
}

// ─── The conclusion ──────────────────────────────────────────────────────────

export type Headline =
  /** The conversion moved beyond noise. */
  | { kind: 'conversion'; change: 'better' | 'worse'; now: number; was: number }
  /** It did not, but a step did: the counts are the fact to start from. */
  | { kind: 'counts' }
  /** Nothing moved beyond noise. */
  | { kind: 'steady' }

export interface StepMove {
  step: Transition
  change: 'better' | 'worse'
  now: number
  was: number
  /** Buyers the change made (+) or cost (−); null when it cannot be priced. */
  buyers: number | null
  z: number
}

/**
 * The mix as it is said: the type named and its share of the site's visitors
 * in both windows. A share of the site, as the breakdown shows it, not of the
 * rows' own sum `mixShift` weighs with — the two differ by a point at most,
 * and the sentence must agree with the table beside it. The share is what
 * moved: in October 2026 against 2025 warm visitors fell from 21 940 to
 * 19 592 while their share rose from 34 % to 56 %, and «тёплых стало больше»
 * said the opposite of the counts.
 */
export interface MixNote extends MixShift {
  shareNow: number
  shareWas: number
}

export interface Conclusion {
  headline: Headline
  buyers: CountChange
  entered: CountChange
  buyersNow: number
  buyersWas: number
  /** For a conversion headline: the step that moved most its way. */
  strongest: StepMove | null
  /** Every other step that moved beyond noise, in the funnel's order. */
  moves: StepMove[]
  mix: MixNote | null
}

/**
 * What the period says against the window it is set beside, as facts in the
 * order they are told: first what happened to the counts or the conversion,
 * then where it happened. Where the site loses most people is not a sentence:
 * the bars mark that step (`biggestLeak`). Only for windows both large enough
 * to claim anything (`enoughToClaim`).
 */
export function conclude(now: FunnelCounts, was: FunnelCounts, byType: FunnelRow[], window: Against): Conclusion {
  const moved: StepMove[] = []
  for (const step of TRANSITIONS) {
    const { z, change } = stepChange(now, was, step)
    const rNow = stepShare(now, step)
    const rWas = stepShare(was, step)
    if (z == null || rNow == null || rWas == null || (change !== 'better' && change !== 'worse')) continue
    moved.push({ step, change, now: rNow, was: rWas, buyers: buyersMoved(now, was, step), z })
  }
  const conv = conversionChange(now, was).change
  const base = {
    buyers: countChange(now.purchased, was.purchased),
    entered: countChange(now.entered, was.entered),
    buyersNow: now.purchased,
    buyersWas: was.purchased,
  }

  if (conv === 'better' || conv === 'worse') {
    // "Moved most" is the step that moved the most buyers, not the surest
    // one: z grows with the people behind a step, and the first step always
    // has the most. October 2026 against 2025, arrival → product was z 15 and
    // about 90 buyers, product → cart z 11 and about 245.
    const weight = (m: StepMove) => Math.abs(m.buyers ?? 0)
    const strongest = moved.filter((m) => m.change === conv)
      .sort((a, b) => weight(b) - weight(a) || Math.abs(b.z) - Math.abs(a.z))[0] ?? null
    const mix = mixShift(byType, window)
    const row = mix ? byType.find((r) => r.key === mix.type) : undefined
    return {
      ...base,
      headline: { kind: 'conversion', change: conv, now: conversion(now) ?? 0, was: conversion(was) ?? 0 },
      strongest,
      moves: moved.filter((m) => m !== strongest && m.change !== conv),
      mix: mix && mix.change === conv && row && now.entered > 0 && was.entered > 0
        ? { ...mix, shareNow: row.current.entered / now.entered, shareWas: row[window].entered / was.entered }
        : null,
    }
  }
  return {
    ...base,
    headline: moved.length ? { kind: 'counts' } : { kind: 'steady' },
    strongest: null,
    moves: moved,
    mix: null,
  }
}

// ─── The breakdown ───────────────────────────────────────────────────────────

/** The row the smallest devices are folded into. */
export const FOLDED_DEVICES = '__folded__'

/** A device under this share of the site's visitors is folded with the
 *  others like it — when there are two or more of them. */
export const FOLD_SHARE = 0.01

export function foldDevices(rows: FunnelRow[], site: FunnelCounts): { rows: FunnelRow[]; folded: string[] } {
  const small = rows.filter((r) => (share(r.current.entered, site.entered) ?? 0) < FOLD_SHARE)
  if (small.length < 2) return { rows, folded: [] }
  const sum = (w: FunnelWindow) => Object.fromEntries(
    FUNNEL_STEPS.map((step) => [step, small.reduce((s, r) => s + r[w][step], 0)]),
  ) as FunnelCounts
  const folded: FunnelRow = { key: FOLDED_DEVICES, current: sum('current'), previous: sum('previous'), year_ago: sum('year_ago') }
  return { rows: [...rows.filter((r) => !small.includes(r)), folded], folded: small.map((r) => r.key) }
}

export interface GroupFigure {
  row: FunnelRow
  /** Of the site's visitors. */
  shareOfSite: number | null
  /** Shown and never judged: too few people, or a fold of several groups. */
  unjudged: boolean
  small: boolean
  /** Each step's share, and whether it is clearly lower than on the rest of
   *  the site. */
  steps: Record<Transition, { share: number | null; lower: boolean }>
  conversion: number | null
  was: number | null
  /** The group's own conversion against the window's, beyond noise. */
  change: Change | null
}

/**
 * A breakdown's rows, each set beside the rest of the site — the site's
 * counts less its own — and beside itself in the window compared with.
 * Rows nobody was in, then or now, are left out.
 *
 * Two judgements, licensed apart: against the rest of the site needs only
 * this period large enough (`judgeNow`), against itself then needs both
 * (`judgeThen`).
 */
export function groupFigures(
  rows: FunnelRow[], site: FunnelCounts, window: Against | null, judgeNow: boolean, judgeThen: boolean,
): GroupFigure[] {
  return rows
    .filter((r) => r.current.entered > 0 || (window != null && r[window].entered > 0))
    .map((row) => {
      const now = row.current
      const small = now.entered < MIN_GROUP_PEOPLE
      const unjudged = small || row.key === FOLDED_DEVICES
      const steps = Object.fromEntries(TRANSITIONS.map((step) => {
        const from = stepBefore(step)
        const lower = judgeNow && !unjudged && judge(noiseZ(
          now[step], now[from], site[step] - now[step], site[from] - now[from],
        )) === 'worse'
        return [step, { share: stepShare(now, step), lower }]
      })) as GroupFigure['steps']
      const was = window ? row[window] : null
      const judgedThen = judgeThen && !unjudged && !!was && was.entered >= MIN_GROUP_PEOPLE
      return {
        row,
        shareOfSite: share(now.entered, site.entered),
        unjudged,
        small,
        steps,
        conversion: conversion(now),
        was: was ? conversion(was) : null,
        change: judgedThen && was ? conversionChange(now, was).change : null,
      }
    })
}

/** How far the rows' visitors fall short of the site's (positive) or pass it
 *  (negative): people with no channel, and people counted in two. */
export function rowsGap(rows: FunnelRow[], site: FunnelCounts): number {
  return site.entered - rows.reduce((s, r) => s + r.current.entered, 0)
}
