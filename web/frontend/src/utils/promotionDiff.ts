/**
 * A promotion's history in words: what changed between two consecutive saved
 * versions — «Перенесено: 5–8 → 6–9 березня», «Знижка: 20 → 25 %»,
 * «Скасовано».
 *
 * Every save is a version (`app.promotion_versions`), so the history is the
 * versions side by side. `diffVersions` says what moved, field by field, as
 * data; `describeChange` says one change in the reader's language. Lists —
 * the items of a scope and the channels — are compared as sets: a brand moved
 * to another place in the list is not a change.
 */
import type {
  PromotionChannel,
  PromotionFields,
  PromotionKind,
  PromotionScope,
  PromotionStatus,
  PromotionVersion,
} from '../types/api'
import i18n from '../lib/i18n'
import type { T } from './plural'
import { formatDiscount, formatRecordedAt, promotionDays, PROMOTION_CHANNELS } from './promotions'
import { kyivToday } from './smsDates'

/** What one version knows: its fields and its status. */
export type VersionState = PromotionFields & { status: PromotionStatus }

interface Days { startsOn: string; endsOn: string }
interface ScopeState { scope: PromotionScope; items: string[] }

export type PromotionChange =
  | { type: 'created' }
  | { type: 'cancelled' }
  | { type: 'restored' }
  | { type: 'moved'; from: Days; to: Days }
  | { type: 'renamed'; from: string; to: string }
  | { type: 'startTime'; from: string | null; to: string | null }
  | { type: 'kind'; from: PromotionKind; to: PromotionKind }
  | { type: 'discount'; from: number | null; to: number | null }
  /** The scope itself changed: the whole store became brands, say. */
  | { type: 'scope'; from: ScopeState; to: ScopeState }
  /** The same scope, other names in it. */
  | { type: 'items'; scope: PromotionScope; added: string[]; removed: string[] }
  | { type: 'channels'; added: PromotionChannel[]; removed: PromotionChannel[] }
  | { type: 'smsCampaign'; from: string | null; to: string | null }
  | { type: 'note'; from: string | null; to: string | null }

// ─── What changed ────────────────────────────────────────────────────────────

const time = (v: string | null | undefined) => (v ? v.slice(0, 5) : null)
const text = (v: string | null | undefined) => (v && v.trim() ? v.trim() : null)
const discount = (v: number | string | null | undefined) =>
  v === null || v === undefined || v === '' ? null : Number(v)

/** What is in `a` and not in `b`, in `a`'s order. */
function minus<V>(a: V[], b: V[]): V[] {
  const other = new Set(b)
  return a.filter((x) => !other.has(x))
}

/** The changes from `prev` to `next`, in the order they are said; `prev`
 *  null is the first version. */
export function diffVersions(prev: VersionState | null, next: VersionState): PromotionChange[] {
  if (!prev) return [{ type: 'created' }]
  const out: PromotionChange[] = []

  if (prev.status !== next.status) {
    out.push({ type: next.status === 'cancelled' ? 'cancelled' : 'restored' })
  }
  if (prev.starts_on !== next.starts_on || prev.ends_on !== next.ends_on) {
    out.push({
      type: 'moved',
      from: { startsOn: prev.starts_on, endsOn: prev.ends_on },
      to: { startsOn: next.starts_on, endsOn: next.ends_on },
    })
  }
  if (prev.name !== next.name) out.push({ type: 'renamed', from: prev.name, to: next.name })
  if (time(prev.start_time) !== time(next.start_time)) {
    out.push({ type: 'startTime', from: time(prev.start_time), to: time(next.start_time) })
  }
  if (prev.kind !== next.kind) out.push({ type: 'kind', from: prev.kind, to: next.kind })
  if (discount(prev.discount_pct) !== discount(next.discount_pct)) {
    out.push({ type: 'discount', from: discount(prev.discount_pct), to: discount(next.discount_pct) })
  }

  if (prev.scope !== next.scope) {
    out.push({
      type: 'scope',
      from: { scope: prev.scope, items: [...prev.scope_items] },
      to: { scope: next.scope, items: [...next.scope_items] },
    })
  } else {
    const added = minus(next.scope_items, prev.scope_items)
    const removed = minus(prev.scope_items, next.scope_items)
    if (added.length || removed.length) out.push({ type: 'items', scope: next.scope, added, removed })
  }

  const channelsAdded = PROMOTION_CHANNELS.filter(
    (c) => next.channels.includes(c) && !prev.channels.includes(c))
  const channelsRemoved = PROMOTION_CHANNELS.filter(
    (c) => prev.channels.includes(c) && !next.channels.includes(c))
  if (channelsAdded.length || channelsRemoved.length) {
    out.push({ type: 'channels', added: channelsAdded, removed: channelsRemoved })
  }

  if (text(prev.sms_campaign) !== text(next.sms_campaign)) {
    out.push({ type: 'smsCampaign', from: text(prev.sms_campaign), to: text(next.sms_campaign) })
  }
  if (text(prev.note) !== text(next.note)) {
    out.push({ type: 'note', from: text(prev.note), to: text(next.note) })
  }
  return out
}

// ─── In words ────────────────────────────────────────────────────────────────

function english(): boolean {
  return i18n.language?.startsWith('en') ?? true
}

const sameMonth = (...dates: string[]) => dates.every((d) => d.slice(0, 7) === dates[0].slice(0, 7))
const dayOf = (iso: string) => String(Number(iso.slice(8, 10)))

/** Both ends of a move. Within one month the month is said once, as people
 *  say it: «5–8 → 6–9 березня». English keeps the month on both sides. */
function moveEnds(from: Days, to: Days, today: string): { from: string; to: string } {
  const after = promotionDays(to.startsOn, to.endsOn, today)
  if (!english() && sameMonth(from.startsOn, from.endsOn, to.startsOn, to.endsOn)) {
    const days = from.startsOn === from.endsOn
      ? dayOf(from.startsOn)
      : `${dayOf(from.startsOn)}–${dayOf(from.endsOn)}`
    return { from: days, to: after }
  }
  return { from: promotionDays(from.startsOn, from.endsOn, today), to: after }
}

const signed = (added: string[], removed: string[]) =>
  [...added.map((v) => `+${v}`), ...removed.map((v) => `−${v}`)].join(', ')

/** One change, in the reader's language. */
export function describeChange(change: PromotionChange, t: T, today: string = kyivToday()): string {
  const percent = (n: number) => t('promo.percent', { value: formatDiscount(n) })
  const scopeText = (s: ScopeState) =>
    s.items.length
      ? t('promo.scopeWithItems', { scope: t(`promo.scope.${s.scope}`), items: s.items.join(', ') })
      : t(`promo.scope.${s.scope}`)

  switch (change.type) {
    case 'created':
      return t('promo.change.created')
    case 'cancelled':
      return t('promo.change.cancelled')
    case 'restored':
      return t('promo.change.restored')
    case 'moved':
      return t('promo.change.moved', moveEnds(change.from, change.to, today))
    case 'renamed':
      return t('promo.change.renamed', { from: change.from, to: change.to })
    case 'startTime':
      return t('promo.change.startTime', {
        from: change.from ?? t('promo.morning'),
        to: change.to ?? t('promo.morning'),
      })
    case 'kind':
      return t('promo.change.kind', {
        from: t(`promo.kind.${change.from}`),
        to: t(`promo.kind.${change.to}`),
      })
    case 'discount': {
      const { from, to } = change
      // «20 → 25 %» when both are said: the sign once, at the end.
      if (from !== null && to !== null) {
        return t('promo.change.discount', { from: formatDiscount(from), to: percent(to) })
      }
      return t('promo.change.discount', {
        from: from === null ? t('promo.notSet') : percent(from),
        to: to === null ? t('promo.notSet') : percent(to),
      })
    }
    case 'scope':
      return t('promo.change.scope', { from: scopeText(change.from), to: scopeText(change.to) })
    case 'items':
      return t('promo.change.items', {
        // «Бренди: +Anua, −COSRX»; the whole store names nothing, but a
        // version that did would still be said.
        scope: change.scope === 'store' ? t('promo.scope.store') : t(`promo.field.${change.scope}`),
        changes: signed(change.added, change.removed),
      })
    case 'channels':
      return t('promo.change.channels', {
        changes: signed(
          change.added.map((c) => t(`promo.channel.${c}`)),
          change.removed.map((c) => t(`promo.channel.${c}`)),
        ),
      })
    case 'smsCampaign':
      if (change.from === null) return t('promo.change.smsAdded', { name: change.to ?? '' })
      if (change.to === null) return t('promo.change.smsRemoved', { name: change.from })
      return t('promo.change.smsChanged', { from: change.from, to: change.to })
    case 'note':
      if (change.from === null) return t('promo.change.noteAdded')
      if (change.to === null) return t('promo.change.noteRemoved')
      return t('promo.change.noteChanged')
  }
}

/** One line of the history: when, whether it was the reader, and what changed. */
export interface HistoryEntry {
  version: number
  when: string
  byMe: boolean
  changes: string[]
}

/** The history in words, newest first, from the versions the API sends
 *  newest first. A save that changed nothing says so rather than nothing. */
export function describeHistory(
  versions: PromotionVersion[], t: T, today: string = kyivToday(),
): HistoryEntry[] {
  return versions.map((v, i) => {
    const prev = versions[i + 1] ?? null
    const changes = diffVersions(prev, v).map((c) => describeChange(c, t, today))
    return {
      version: v.version,
      when: formatRecordedAt(v.recorded_at, today),
      byMe: v.recorded_by_me,
      changes: changes.length ? changes : [t('promo.change.none')],
    }
  })
}
