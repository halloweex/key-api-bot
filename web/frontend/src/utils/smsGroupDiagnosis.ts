/**
 * Which condition emptied a group.
 *
 * A group that matches nobody used to say only that: «Под условия этой группы
 * не подходит никто», and the person was left to find, among three sentences,
 * the one that did it — on 2026-10-17 it was a 92 typed as an order count. The
 * page now asks the preview what the group would match WITHOUT each of its
 * conditions, one at a time, and names the one whose removal brings people
 * back.
 *
 * Asked on the page, not counted by the server. A per-condition count in the
 * audience query would ride every preview for free, but the group's predicate
 * is one rendered clause with positional parameters, held to the frozen
 * pre-groups query on two engines; splitting it per condition is a change to
 * the one query every campaign is frozen from, for a message. These previews
 * cost nothing until a group is empty: at most one per condition (N for a
 * group of N, none for a group of one, whose only condition is the answer),
 * asked in turn after the audience's own count has settled, and cached.
 *
 * Two ways for a group to bring nobody, read from the group's share:
 *
 * - `nobody` — its own conditions match no customer (`matched` is 0). The
 *   probes read `matched` too, so the campaign's common rules play no part.
 * - `unreachable` — they match people none of whom can be messaged (no valid
 *   number, opted out, or outside the campaign's value level). The probes
 *   read the people reached, and the level is probed as one more "condition".
 */
import { audienceToParams } from './smsAudience'
import {
  conditionProblem, definitionToCriteria, newId,
  type AudienceDefinition, type Condition,
} from './smsConditions'
import type { SmsAudienceGroupShare, SmsSegmentsResponse } from '../types/api'

export type EmptyKind = 'nobody' | 'unreachable'

/** The probe that asks about the campaign's value level, not a condition. */
export const LEVEL = 'level'

export function emptyKind(share: SmsAudienceGroupShare | undefined): EmptyKind | null {
  if (!share) return null
  if (share.matched === 0) return 'nobody'
  if (share.recipients === 0) return 'unreachable'
  return null
}

/** The conditions the query applies: an unfinished one is left out of it. */
function applied(def: AudienceDefinition, index: number): Condition[] {
  return (def.groups[index]?.conditions ?? []).filter((c) => !conditionProblem(c))
}

export interface DiagnosisProbe {
  /** Unique across the page's probes, for the count cache. */
  key: string
  /** The condition left out, or `LEVEL`. */
  without: string
  params: string
}

/**
 * The previews that would say which condition emptied group `index`: the
 * group alone, once without each of its conditions. None are needed for a
 * group of one condition that matches nobody — that condition is the answer.
 */
export function diagnosisProbes(
  def: AudienceDefinition, index: number, kind: EmptyKind, today?: string,
): DiagnosisProbe[] {
  const conditions = applied(def, index)
  if (kind === 'nobody' && conditions.length < 2) return []
  const ask = (without: string, groupConditions: Condition[], tiers = def.tiers) => ({
    key: `diagnose:${index}:${without}`,
    without,
    params: audienceToParams(definitionToCriteria({
      ...def, tiers, groups: [{ id: newId('g'), conditions: groupConditions }],
    }, today)),
  })
  const probes = conditions.map((c) => ask(c.id, conditions.filter((x) => x !== c)))
  if (kind === 'unreachable' && def.tiers.length > 0) probes.push(ask(LEVEL, conditions, []))
  return probes
}

/** How many a probe found, in the terms of `kind`. */
export function probeCount(kind: EmptyKind, data: SmsSegmentsResponse): number {
  return kind === 'nobody' ? (data.groups?.[0]?.matched ?? 0) : data.totals.customers
}

export type GroupDiagnosis =
  /** This condition matches nobody at all. */
  | { kind: 'alone'; conditionId: string }
  /** With the others it matches nobody; without it, `without` would match. */
  | { kind: 'cuts'; conditionId: string; without: number }
  /** Any one of these left out brings people back: they exclude each other. */
  | { kind: 'clash'; conditionIds: string[] }
  /** Two conditions, and neither matches anybody on its own. */
  | { kind: 'neither'; conditionIds: [string, string] }
  /** No one condition is to blame; it takes the group as a whole. */
  | { kind: 'together' }
  /** Unreachable: the campaign's value level leaves nobody to message. */
  | { kind: 'level'; without: number }
  /** Unreachable: without this condition, `without` could be messaged. */
  | { kind: 'reach'; conditionId: string; without: number }

/**
 * The answer, from the probes' counts by condition id (`undefined` while one
 * is being asked, `null` if it failed). `'pending'` until every count is in;
 * null when nothing can be said — a probe failed, or no condition explains it,
 * and the card keeps its plain sentence.
 */
export function diagnoseGroup(
  def: AudienceDefinition, index: number, kind: EmptyKind,
  counts: Record<string, number | null | undefined>,
): GroupDiagnosis | 'pending' | null {
  const conditions = applied(def, index)
  if (conditions.length === 0) return null
  if (kind === 'nobody' && conditions.length === 1) {
    return { kind: 'alone', conditionId: conditions[0].id }
  }
  const asked = [...conditions.map((c) => c.id),
    ...(kind === 'unreachable' && def.tiers.length > 0 ? [LEVEL] : [])]
  if (asked.some((id) => counts[id] === undefined)) return 'pending'
  if (asked.some((id) => counts[id] === null)) return null
  const n = (id: string) => counts[id] as number

  if (kind === 'unreachable') {
    if (asked.includes(LEVEL) && n(LEVEL) > 0) return { kind: 'level', without: n(LEVEL) }
    const best = conditions.map((c) => c.id).sort((a, b) => n(b) - n(a))[0]
    return n(best) > 0 ? { kind: 'reach', conditionId: best, without: n(best) } : null
  }

  const freeing = conditions.filter((c) => n(c.id) > 0).map((c) => c.id)
  if (freeing.length === 1) {
    const [culprit] = freeing
    // Of two conditions, the group without the OTHER is this one alone: if
    // that matched nobody, it matches nobody at all.
    if (conditions.length === 2) {
      const other = conditions.find((c) => c.id !== culprit)!.id
      if (n(other) === 0) return { kind: 'alone', conditionId: culprit }
    }
    return { kind: 'cuts', conditionId: culprit, without: n(culprit) }
  }
  if (freeing.length > 1) return { kind: 'clash', conditionIds: freeing }
  if (conditions.length === 2) {
    return { kind: 'neither', conditionIds: [conditions[0].id, conditions[1].id] }
  }
  return { kind: 'together' }
}
