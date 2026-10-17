/**
 * The audience said in words: one phrase per condition, one sentence for the
 * whole audience.
 *
 * Generated from the definition, never written by hand and never by a model —
 * the sentence under the groups has to say exactly what the query will do, or
 * it is one more thing on the screen that can disagree with the count.
 *
 * Plurals without plural keys: the three dictionaries must carry the same key
 * set (`tests/unit/test_frontend_locales.py`), and Russian has four plural
 * categories where English has two. So a counted word is ONE key holding its
 * forms separated by `|` — "order|orders", «заказ|заказа|заказов» — and
 * `plural` picks the form with `Intl.PluralRules`.
 */
import i18n, { LANGUAGE_LOCALES, type SupportedLanguage } from '../lib/i18n'
import { formatCurrency, formatNumber } from './formatters'
import { formatDay, isIsoDate } from './smsDates'
import {
  conditionProblem, definitionFromFrozen, SOURCES,
  type AudienceDefinition, type BoughtCondition, type Condition, type ConditionGroup,
  type OrdersCondition, type PeriodValue, type SourceCondition, type WhenValue,
} from './smsConditions'
import type { SmsTier } from '../types/api'

export type T = (key: string, opts?: Record<string, unknown>) => string

/** What a phrase may need from the page: the names behind ids. */
export interface PhraseContext {
  categoryName?: (id: number) => string | undefined
}

function locale(): string {
  return LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'en-US'
}

/**
 * The form of a counted word for `n`. Forms are `one|other` in English and
 * `one|few|many` in Ukrainian and Russian; a key without `|` is used as is.
 */
export function plural(t: T, key: string, n: number): string {
  const forms = t(key).split('|')
  if (forms.length === 1) return forms[0]
  const rule = new Intl.PluralRules(locale()).select(n)
  if (forms.length === 2) return rule === 'one' ? forms[0] : forms[1]
  if (rule === 'one') return forms[0]
  if (rule === 'few') return forms[1]
  if (rule === 'many') return forms[2]
  // Fractions; never a count of orders, but never a crash either.
  return forms[1]
}

const blank = '…'
const shown = (n: number | null | undefined) =>
  (typeof n === 'number' && Number.isFinite(n) ? formatNumber(n) : blank)
const forCount = (n: number | null | undefined) => (typeof n === 'number' ? n : 5)

// ─── Pieces ──────────────────────────────────────────────────────────────────

/** «2+ заказа», «ровно 1 заказ», «не больше 3 заказов», «ни одного заказа». */
export function countPhrase(c: OrdersCondition, t: T): string {
  switch (c.op) {
    case 'atLeast':
      return t('sms.cond.count.atLeast', {
        n: shown(c.count), word: plural(t, 'word.orders', forCount(c.count)),
      })
    case 'exactly':
      return t('sms.cond.count.exactly', {
        n: shown(c.count), word: plural(t, 'word.orders', forCount(c.count)),
      })
    case 'atMost':
      return t('sms.cond.count.atMost', {
        n: shown(c.count), word: plural(t, 'word.ordersGen', forCount(c.count)),
      })
    case 'between':
      return t('sms.cond.count.between', {
        n: shown(c.count), m: shown(c.countMax),
        word: plural(t, 'word.ordersGen', forCount(c.countMax)),
      })
    case 'none':
      return t('sms.cond.count.none')
  }
}

/** «с 1 янв по 14 окт 2026», «с 1 янв 2026», «по 14 окт 2026». */
function fromTo(from: string, to: string, t: T): string {
  const a = isIsoDate(from) ? from : ''
  const b = isIsoDate(to) ? to : ''
  if (a && b) {
    const sameYear = a.slice(0, 4) === b.slice(0, 4)
    return t('sms.cond.fromTo', { from: formatDay(a, { withYear: !sameYear }), to: formatDay(b) })
  }
  if (a) return t('sms.cond.since', { from: formatDay(a) })
  if (b) return t('sms.cond.until', { to: formatDay(b) })
  return t('sms.cond.datesUnset')
}

/** «за всё время», «за последние 90 дней», «с 1 янв по 14 окт 2026». */
export function periodPhrase(p: PeriodValue, t: T): string {
  if (p.kind === 'all') return t('sms.cond.period.all')
  if (p.kind === 'last') {
    return t('sms.cond.period.last', {
      n: shown(p.days), word: plural(t, 'word.days', forCount(p.days)),
    })
  }
  return fromTo(p.from, p.to, t)
}

/** «в последние 90 дней», «60+ дней назад», «90–180 дней назад». */
export function whenPhrase(w: WhenValue, t: T): string {
  switch (w.kind) {
    case 'within':
      return t('sms.cond.when.within', {
        n: shown(w.days), word: plural(t, 'word.days', forCount(w.days)),
      })
    case 'before':
      return t('sms.cond.when.before', {
        n: shown(w.days), word: plural(t, 'word.days', forCount(w.days)),
      })
    case 'ago':
      if (w.min != null && w.max == null) return whenPhrase({ kind: 'before', days: w.min }, t)
      if (w.max != null && w.min == null) return whenPhrase({ kind: 'within', days: w.max }, t)
      return t('sms.cond.when.ago', {
        min: shown(w.min), max: shown(w.max),
        word: plural(t, 'word.days', forCount(w.max)),
      })
    case 'dates':
      return fromTo(w.from, w.to, t)
  }
}

/** «от ₴5 000 до ₴50 000», «от ₴5 000», «до ₴50 000». */
export function moneyPhrase(min: number | null, max: number | null, t: T): string {
  if (min != null && max != null) {
    return t('sms.cond.money.between', { min: formatCurrency(min), max: formatCurrency(max) })
  }
  if (min != null) return t('sms.cond.money.from', { min: formatCurrency(min) })
  if (max != null) return t('sms.cond.money.to', { max: formatCurrency(max) })
  return t('sms.cond.money.unset')
}

function list(items: string[], t: T): string {
  if (!items.length) return blank
  if (items.length === 1) return items[0]
  return `${items.slice(0, -1).join(', ')} ${t('sms.cond.orWord')} ${items[items.length - 1]}`
}

/** A sales channel by name: «Instagram», «Сайт», «Выставка». */
export function sourceLabel(id: number, t: T): string {
  return SOURCES.includes(id) ? t(`sms.source.${id}`) : `#${id}`
}

function genderPhrase(genders: Array<'f' | 'm'>, t: T): string {
  return list(genders.map((g) => t(g === 'f' ? 'sms.genderFemale' : 'sms.genderMale')), t)
}

// ─── A condition, for its row ────────────────────────────────────────────────

/**
 * A row's sentence: the fixed words, and the blanks that open the editor.
 * «Сделал(а)» [2+ заказа] [с 1 янв по 14 окт 2026].
 */
export function conditionSentence(
  c: Condition, t: T, ctx: PhraseContext = {},
): { label: string; blanks: string[] } {
  const label = t(`sms.cond.label.${c.type}`)
  switch (c.type) {
    case 'orders':
      return { label, blanks: [countPhrase(c, t), periodPhrase(c.period, t)] }
    case 'lastOrder':
    case 'firstOrder':
      return { label, blanks: [whenPhrase(c.when, t)] }
    case 'spent':
    case 'aov':
      return { label, blanks: [moneyPhrase(c.min, c.max, t)] }
    case 'bought': {
      const what: string[] = []
      if (c.brands.length) what.push(list(c.brands, t))
      if (c.categoryIds.length) {
        what.push(t('sms.cond.bought.category', {
          names: list(c.categoryIds.map((id) => ctx.categoryName?.(id) ?? `#${id}`), t),
        }))
      }
      if (c.promocode.trim()) {
        what.push(t('sms.cond.bought.promo', { code: c.promocode.trim() }))
      }
      return {
        label,
        blanks: [
          what.length ? what.join(' ') : t('sms.cond.bought.unset'),
          c.withinDays != null
            ? periodPhrase({ kind: 'last', days: c.withinDays }, t)
            : t('sms.cond.period.all'),
        ],
      }
    }
    case 'source':
      return {
        label,
        blanks: [c.sourceIds.length
          ? list(c.sourceIds.map((id) => sourceLabel(id, t)), t)
          : t('sms.cond.pickSome')],
      }
    case 'city':
      return { label, blanks: [c.cities.length ? list(c.cities, t) : t('sms.cond.pickSome')] }
    case 'gender': {
      const who = c.genders.length ? genderPhrase(c.genders, t) : t('sms.cond.gender.any')
      return {
        label,
        blanks: c.confidence ? [who, t(`sms.cond.gender.conf.${c.confidence}`)] : [who],
      }
    }
  }
}

// ─── The whole audience ──────────────────────────────────────────────────────

/** One condition as a clause of the summary: «2+ заказа с 1 янв по 14 окт 2026». */
export function conditionClause(c: Condition, t: T, ctx: PhraseContext = {}): string {
  const { blanks } = conditionSentence(c, t, ctx)
  switch (c.type) {
    case 'orders':
      return t('sms.sum.orders', { count: blanks[0], period: blanks[1] })
    case 'bought':
      return c.withinDays != null
        ? t('sms.sum.boughtWithin', { what: blanks[0], period: blanks[1] })
        : t('sms.sum.bought', { what: blanks[0] })
    case 'gender':
      return t('sms.sum.gender', { what: blanks.join(' ') })
    default:
      return t(`sms.sum.${c.type}`, { what: blanks[0] })
  }
}

/**
 * A group as one clause, its conditions joined by "and"; null when it is
 * everyone.
 *
 * "Bought" and "ordered via" in one group are ONE clause, because the server
 * asks both of one order line (one EXISTS, under the same "within N days"):
 * «покупки Anua через Instagram за последние 30 дней» — not two independent
 * facts joined by "and", which is what two clauses would claim.
 */
export function groupClause(g: ConditionGroup, t: T, ctx: PhraseContext = {}): string | null {
  const applied = g.conditions.filter((c) => conditionProblem(c) === null)
  if (!applied.length) return null
  const bought = applied.find((c): c is BoughtCondition => c.type === 'bought')
  const source = applied.find((c): c is SourceCondition => c.type === 'source')
  return applied
    .filter((c) => !(bought && c === source))
    .map((c) => {
      if (c !== bought || !source) return conditionClause(c, t, ctx)
      const [what, period] = conditionSentence(bought, t, ctx).blanks
      const via = conditionSentence(source, t, ctx).blanks[0]
      return bought.withinDays != null
        ? t('sms.sum.boughtViaWithin', { what, via, period })
        : t('sms.sum.boughtVia', { what, via })
    })
    .join(t('sms.sum.and'))
}

function tierList(tiers: SmsTier[], t: T): string {
  return list(tiers.map((tier) => t(`sms.tier.${tier}`)), t)
}

/**
 * «Клиенты, у которых 2+ заказа с 1 янв по 14 окт 2026, или 1+ заказ с 16 июл
 * по 14 окт 2026 — 4 101 человек».
 *
 * A group with nothing in it is every buyer, and every buyer OR anything is
 * every buyer, so one empty group says the whole sentence.
 */
export function audienceSentence(
  def: AudienceDefinition, t: T,
  { count, ...ctx }: PhraseContext & { count?: number } = {},
): string {
  const clauses = def.groups.map((g) => groupClause(g, t, ctx))
  let head = !clauses.length || clauses.some((c) => c === null)
    ? t('sms.sum.everyone')
    : `${t('sms.sum.prefix')} ${clauses.join(t('sms.sum.or'))}`
  if (def.tiers.length) head += t('sms.sum.tiers', { tiers: tierList(def.tiers, t) })
  if (count == null) return head
  return t('sms.sum.withCount', {
    sentence: head, count: formatNumber(count), word: plural(t, 'word.people', count),
  })
}

/** How the campaign is measured, in one line: «Одним плечом · По марже · контроль 10%». */
export function describeMeasurement(
  m: { grouping: string; ltvBasis?: string; holdoutPct?: number },
  t: T,
): string {
  const parts = [t(`sms.grouping.${m.grouping === 'rfm' ? 'rfm' : 'single'}`)]
  if (m.ltvBasis) parts.push(t(m.ltvBasis === 'margin' ? 'sms.basisMargin' : 'sms.basisRevenue'))
  if (typeof m.holdoutPct === 'number') parts.push(t('sms.summaryHoldout', { pct: m.holdoutPct }))
  return parts.join(' · ')
}

/**
 * A frozen campaign's audience, in the same words the wizard used to build
 * it: the measurement on the first line, then the people — one line per
 * group when there are several.
 *
 * Read loosely: a campaign frozen by an older page must still describe itself.
 * A follow-up has no audience of its own and says only how it is measured.
 */
export function describeFrozenCriteria(
  criteria: Record<string, unknown> | undefined, t: T, ctx: PhraseContext = {},
): string[] {
  if (!criteria) return []
  const head = describeMeasurement({
    grouping: criteria.grouping === 'rfm' ? 'rfm' : 'single',
    ltvBasis: typeof criteria.ltvBasis === 'string' ? criteria.ltvBasis : undefined,
    holdoutPct: typeof criteria.holdoutPct === 'number' ? criteria.holdoutPct : undefined,
  }, t)
  if (typeof criteria.followUpOf === 'string') return [head]
  const def = definitionFromFrozen(criteria)
  if (def.groups.length > 1) {
    return [head, ...def.groups.map((g, i) =>
      `${t('sms.groupTitle', { n: i + 1 })}: ${groupClause(g, t, ctx) ?? t('sms.sum.everyone')}`)]
  }
  return [head, audienceSentence(def, t, ctx)]
}
