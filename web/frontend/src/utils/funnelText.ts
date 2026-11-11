/**
 * The site funnel said in words: its numbers formatted, its conclusion in
 * sentences.
 *
 * Generated from the arithmetic in `utils/funnel`, never written by hand and
 * never by a model: a sentence on the page has to say exactly what the
 * numbers under it say. Fact first, then where, and no "because" — the
 * funnel shows where people stop, not why.
 */
import i18n, { LANGUAGE_LOCALES, type SupportedLanguage } from '../lib/i18n'
import { formatNumber } from './formatters'
import { plural, type T } from './plural'
import { formatDay, kyivToday } from './smsDates'
import {
  conversion, perBase, roughly,
  type Change, type Conclusion, type CountChange, type MixNote, type StepMove,
} from './funnel'
import type { FunnelCounts } from '../types/api'

function locale(): string {
  return LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'en-US'
}

function pct(fraction: number, digits: number): string {
  return new Intl.NumberFormat(locale(), {
    style: 'percent', minimumFractionDigits: digits, maximumFractionDigits: digits,
  }).format(fraction)
}

const known = (f: number | null | undefined): f is number => typeof f === 'number' && Number.isFinite(f)

/** A step's share: one decimal under 20 %, whole above — «12,9 %», «55 %». */
export function stepPct(f: number | null | undefined): string {
  return known(f) ? pct(f, f < 0.2 ? 1 : 0) : '—'
}

/** The funnel's total, to two decimals — «2,02 %». */
export function convPct(f: number | null | undefined): string {
  return known(f) ? pct(f, 2) : '—'
}

/** A group's share of the site's visitors — «56 %», «0,5 %». */
export function groupPct(f: number | null | undefined): string {
  return known(f) ? pct(f, f < 0.01 ? 1 : 0) : '—'
}

/** A count's change, without its sign — «13 %». */
export function movePct(ratio: number | null): string {
  return pct(Math.abs(ratio ?? 0), 0)
}

// ─── The conclusion ──────────────────────────────────────────────────────────

export type Tone = 'good' | 'bad' | 'neutral'

/** One sentence; `lead` is its opening words, set apart and coloured. */
export interface Line {
  tone: Tone
  lead?: string
  text: string
}

export function toneOf(change: Change | null | undefined): Tone {
  return change === 'better' ? 'good' : change === 'worse' ? 'bad' : 'neutral'
}

function clause(c: CountChange, who: 'buyers' | 'entered', t: T): string {
  if (c.move === 'same') return t(`traffic.funnel.counts.${who}Same`)
  return t(`traffic.funnel.counts.${who}${c.move === 'more' ? 'More' : 'Fewer'}`, { pct: movePct(c.ratio) })
}

/** «Покупателей почти столько же, а зашедших на 13 % больше». */
export function countsSentence(buyers: CountChange, entered: CountChange, t: T): string {
  if (buyers.move === 'same' && entered.move === 'same') return t('traffic.funnel.counts.bothSame')
  const a = clause(buyers, 'buyers', t)
  const b = clause(entered, 'entered', t)
  return t(buyers.move === entered.move ? 'traffic.funnel.counts.and' : 'traffic.funnel.counts.but', { a, b })
}

/** What a step's change made or cost, in buyers: «— минус около 80
 *  покупателей». A sign, not «это»: «это около 80 покупателей» beside a step
 *  read as the people at that step. */
function cost(move: StepMove, t: T): string {
  if (move.buyers == null) return ''
  const n = roughly(move.buyers)
  if (n === 0) return ''
  return t(move.buyers < 0 ? 'traffic.funnel.costLost' : 'traffic.funnel.costGained', {
    n: formatNumber(n), word: plural(t, 'word.buyersGen', n),
  })
}

/** «▼ Просел один шаг: корзина → оформление — минус около 80 покупателей.»
 *  The step's own shares stand under its bar, so they are not said again. */
function moveLine(move: StepMove, lead: 'one' | 'many', t: T): Line {
  const better = move.change === 'better'
  return {
    tone: better ? 'good' : 'bad',
    lead: t(`traffic.funnel.lead.${lead === 'one' ? 'one' : 'many'}${better ? 'Better' : 'Worse'}`),
    text: `${t(`traffic.funnel.pair.${move.step}`)}${cost(move, t)}.`,
  }
}

/**
 * The conclusion as plain sentences, and nothing the screen already says
 * (the owner, 2026-11-05: «слишком много текста… ты теряешься»). No headline:
 * «Конверсия упала: было X, стало Y» and «Всё в пределах колебаний» repeated
 * the number above them. No sentence on where people leave: the bars mark
 * that step. What is left is what nothing else on the screen says — the
 * counts, each step that moved beyond noise with what it cost, and the mix.
 * Every step that moved gets its own sentence; none is cut.
 */
export function sayConclusion(c: Conclusion, t: T): Line[] {
  const lines: Line[] = []
  if (c.headline.kind === 'conversion') {
    if (c.strongest) {
      const s = c.strongest
      lines.push({
        tone: 'neutral',
        text: `${t(`traffic.funnel.strongest.${s.change}`, { pair: t(`traffic.funnel.pair.${s.step}`) })}${cost(s, t)}.`,
      })
    }
    for (const m of c.moves) lines.push(moveLine(m, 'many', t))
    if (c.mix) {
      lines.push({
        tone: 'neutral',
        text: t(`traffic.funnel.mix.${c.mix.change === 'better' ? 'up' : 'down'}`, {
          who: t(`traffic.funnel.mixWho.${c.mix.type}${c.mix.more ? 'More' : 'Fewer'}`, {
            from: groupPct(c.mix.shareWas), to: groupPct(c.mix.shareNow),
          }),
        }),
      })
    }
    return lines
  }
  if (c.headline.kind === 'counts' || c.buyers.move !== 'same' || c.entered.move !== 'same') {
    lines.push({ tone: 'neutral', text: `${countsSentence(c.buyers, c.entered, t)}.` })
  }
  for (const m of c.moves) lines.push(moveLine(m, c.moves.length === 1 ? 'one' : 'many', t))
  return lines
}

/** The mix in one line, for a phone, where it stays under the number while
 *  the rest of the conclusion moves below the funnel: a green «лучше
 *  обычного» with nothing beside it would read as the whole story.
 *  «Часть роста дал состав трафика: тёплых 56 % вместо 34 %.» */
export function sayMixShort(mix: MixNote, t: T): string {
  return t(`traffic.funnel.mixShort.${mix.change === 'better' ? 'up' : 'down'}`, {
    type: t(`traffic.funnel.mixShortType.${mix.type}`),
    to: groupPct(mix.shareNow),
    from: groupPct(mix.shareWas),
  })
}

// ─── The comparison ──────────────────────────────────────────────────────────

const atNoon = (iso: string) => new Date(`${iso}T12:00:00Z`)

/** A month's short name after a day — the form a date takes («21 мая»,
 *  «21 июн.»), not the nominative a month alone takes («май»). */
function monthAfterDay(iso: string): string {
  return new Intl.DateTimeFormat(locale(), { day: 'numeric', month: 'short', timeZone: 'UTC' })
    .formatToParts(atNoon(iso)).find((p) => p.type === 'month')?.value ?? ''
}

/**
 * The window the period is compared with, short enough for a chip beside the
 * block's title on a phone: «21–31 окт.», «31 авг. – 30 сент.», «окт. 2025».
 * A month is named once, and a year only when it is not this one.
 */
export function compareChip(start: string, end: string, today: string = kyivToday()): string {
  const year = end.slice(0, 4) === today.slice(0, 4) ? '' : ` ${end.slice(0, 4)}`
  const day = (iso: string) => String(Number(iso.slice(8, 10)))
  const lastDay = new Date(Date.UTC(Number(end.slice(0, 4)), Number(end.slice(5, 7)), 0)).getUTCDate()
  const sameMonth = start.slice(0, 7) === end.slice(0, 7)
  const english = locale().startsWith('en')
  if (sameMonth && start.slice(8) === '01' && Number(end.slice(8)) === lastDay) {
    const month = new Intl.DateTimeFormat(locale(), { month: 'short', timeZone: 'UTC' }).format(atNoon(start))
    return `${month}${year}`
  }
  if (sameMonth) {
    return english
      ? `${monthAfterDay(end)} ${day(start)}–${day(end)}${year}`
      : `${day(start)}–${day(end)} ${monthAfterDay(end)}${year}`
  }
  return english
    ? `${monthAfterDay(start)} ${day(start)} – ${monthAfterDay(end)} ${day(end)}${year}`
    : `${day(start)} ${monthAfterDay(start)} – ${day(end)} ${monthAfterDay(end)}${year}`
}

// ─── The headline number ─────────────────────────────────────────────────────

/** «из 1 000 зашедших купили 20». */
export function perBaseText(now: FunnelCounts, t: T): string | null {
  const rate = conversion(now)
  if (rate == null) return null
  const { base, count } = perBase(rate)
  return t('traffic.funnel.hero.per', { base: formatNumber(base), n: formatNumber(count) })
}

export interface HeroWas {
  /** «было 2,33 %». */
  was: string
  /** «в пределах колебаний», «хуже обычного», «лучше обычного»; null when
   *  the windows were too small to judge. */
  verdict: string | null
  tone: Tone
}

/** What the conversion was and the verdict on it, as two pieces set beside the
 *  number rather than one sentence under it. */
export function heroWas(was: number | null, change: Change | null, t: T): HeroWas | null {
  if (was == null) return null
  return {
    was: t('traffic.funnel.hero.was', { was: convPct(was) }),
    verdict: change ? t(`traffic.funnel.hero.verdict.${change}`) : null,
    tone: toneOf(change),
  }
}

// ─── The breakdown ───────────────────────────────────────────────────────────

/** Where the breakdown's rows fall short of the site, or pass it, in words. */
export function gapText(gap: number, by: 'type' | 'channel' | 'device', t: T): string | null {
  if (gap === 0) return null
  const what = by === 'device' ? 'Device' : 'Channel'
  const n = Math.abs(gap)
  return gap > 0
    ? t(`traffic.funnel.table.short${what}`, { n: formatNumber(n), word: plural(t, 'word.peopleGen', n) })
    : t(`traffic.funnel.table.over${what}`, { n: formatNumber(n) })
}

/** When the figures on screen were fetched, in Kyiv: «10:42» today, or
 *  «3 ноября, 10:42» on another day. */
export function fetchedAt(iso: string, today: string = kyivToday()): string {
  const at = new Date(iso)
  if (Number.isNaN(at.getTime())) return iso
  const time = new Intl.DateTimeFormat(locale(), {
    timeZone: 'Europe/Kyiv', hour: '2-digit', minute: '2-digit',
  }).format(at)
  const day = kyivToday(at)
  return day === today ? time : `${formatDay(day, { withYear: false, month: 'long' })}, ${time}`
}
