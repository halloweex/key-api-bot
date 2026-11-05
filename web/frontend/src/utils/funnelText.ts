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
  type Change, type Conclusion, type CountChange, type StepMove, type Transition,
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

function cost(move: StepMove, t: T): string {
  if (move.buyers == null) return ''
  const n = roughly(move.buyers)
  if (n === 0) return ''
  const word = move.buyers < 0 ? 'word.buyersGen' : 'word.extraBuyersGen'
  return t('traffic.funnel.cost', { n: formatNumber(n), word: plural(t, word, n) })
}

function moveLine(move: StepMove, lead: 'one' | 'many', t: T): Line {
  const better = move.change === 'better'
  return {
    tone: better ? 'good' : 'bad',
    lead: t(`traffic.funnel.lead.${lead === 'one' ? 'one' : 'many'}${better ? 'Better' : 'Worse'}`),
    text: `${t(`traffic.funnel.move.${move.step}`, { now: stepPct(move.now), was: stepPct(move.was) })}${cost(move, t)}.`,
  }
}

export interface Said {
  headline: Line
  lines: Line[]
}

export function sayConclusion(c: Conclusion, t: T): Said {
  const lines: Line[] = []
  let headline: Line

  if (c.headline.kind === 'conversion') {
    const up = c.headline.change === 'better'
    headline = {
      tone: up ? 'good' : 'bad',
      text: t(`traffic.funnel.headline.${up ? 'conversionUp' : 'conversionDown'}`, {
        was: convPct(c.headline.was), now: convPct(c.headline.now),
      }),
    }
    if (c.strongest) {
      const s = c.strongest
      lines.push({
        tone: 'neutral',
        text: `${t(`traffic.funnel.strongest.${s.change}`, {
          pair: t(`traffic.funnel.pair.${s.step}`), was: stepPct(s.was), now: stepPct(s.now),
        })}${cost(s, t)}.`,
      })
    }
    for (const m of c.moves) lines.push(moveLine(m, 'many', t))
    if (c.mix) {
      lines.push({
        tone: 'neutral',
        text: t(`traffic.funnel.mix.${c.mix.change === 'better' ? 'up' : 'down'}`, {
          who: t(`traffic.funnel.mixWho.${c.mix.type}${c.mix.more ? 'More' : 'Fewer'}`),
        }),
      })
    }
  } else if (c.headline.kind === 'counts') {
    headline = { tone: 'neutral', text: countsSentence(c.buyers, c.entered, t) }
    const detail = c.buyers.move === 'same' && c.entered.move === 'more' ? 'extraBoughtNothing'
      : c.buyers.move === 'same' && c.entered.move === 'fewer' ? 'lostBoughtNothing'
        : 'plain'
    lines.push({
      tone: 'neutral',
      text: t(`traffic.funnel.detail.${detail}`, {
        now: formatNumber(c.buyersNow), word: plural(t, 'word.buyers', c.buyersNow), was: formatNumber(c.buyersWas),
      }),
    })
    for (const m of c.moves) lines.push(moveLine(m, c.moves.length === 1 ? 'one' : 'many', t))
  } else {
    headline = { tone: 'neutral', text: t('traffic.funnel.headline.steady') }
    if (c.buyers.move !== 'same' || c.entered.move !== 'same') {
      lines.push({ tone: 'neutral', text: `${countsSentence(c.buyers, c.entered, t)}.` })
    }
  }

  const leakLine = sayLeak(c.leak, t)
  if (leakLine) lines.push(leakLine)
  return { headline, lines }
}

export function sayLeak(leak: Conclusion['leak'], t: T): Line | null {
  if (!leak) return null
  const where = t(`traffic.funnel.between.${leak.step}`)
  const share = stepPct(leak.leftShare)
  const text = leak.asBefore ? t('traffic.funnel.leak.asBefore', { where, share })
    : leak.was != null ? t('traffic.funnel.leak.was', { where, share, was: stepPct(leak.was) })
      : t('traffic.funnel.leak.plain', { where, share })
  return { tone: 'neutral', text }
}

// ─── The headline number ─────────────────────────────────────────────────────

/** «из 1 000 зашедших купили 20». */
export function perBaseText(now: FunnelCounts, t: T): string | null {
  const rate = conversion(now)
  if (rate == null) return null
  const { base, count } = perBase(rate)
  return t('traffic.funnel.hero.per', { base: formatNumber(base), n: formatNumber(count) })
}

/** «было 2,33 % — в пределах обычных колебаний»; the verdict only when judged. */
export function wasText(was: number | null, change: Change | null, t: T, form: 'hero' | 'total'): Line | null {
  if (was == null) return null
  const verdict = change === 'worse' ? 'Worse' : change === 'better' ? 'Better' : change === 'same' ? 'Same' : 'Plain'
  return { tone: toneOf(change), text: t(`traffic.funnel.${form}.was${verdict}`, { was: convPct(was) }) }
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

/** The step a transition names, for the words between the bars. */
export function transitionText(step: Transition, t: T, short: boolean): string {
  return t(`traffic.funnel.${short ? 'transitionShort' : 'transition'}.${step}`)
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
