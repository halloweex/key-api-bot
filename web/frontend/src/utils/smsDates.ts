/**
 * Calendar days for the SMS audience, in Kyiv.
 *
 * The server counts recency and "within N days" from the Kyiv date
 * (`TODAY_IN_KYIV`), so the page has to convert between days and dates on the
 * same calendar. The browser's own date would be a day off for three hours
 * every night for anybody whose laptop is not set to Kyiv — and a day off at
 * the edge of a period is somebody's last order in or out of the audience.
 *
 * Dates travel as ISO `YYYY-MM-DD` strings, the shape a native
 * `<input type="date">` reads and writes. Arithmetic is done on UTC midnights,
 * where a day is always 86 400 000 ms; doing it on local midnights loses an
 * hour on the day the clocks change.
 */
import i18n, { LANGUAGE_LOCALES, type SupportedLanguage } from '../lib/i18n'

const DAY_MS = 86_400_000
const ISO = /^(\d{4})-(\d{2})-(\d{2})$/

/**
 * Is this a real calendar date in ISO form? `2026-02-30` is not — and neither
 * is `0002-10-14`, which is what a native date field holds while the year is
 * still being typed. Treating that as a date would swap a range mid-keystroke
 * and send the server a recency of seven hundred thousand days.
 */
export function isIsoDate(value: unknown): value is string {
  if (typeof value !== 'string') return false
  const m = ISO.exec(value)
  if (!m) return false
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])]
  if (y < 1900 || y > 2100) return false
  const at = new Date(Date.UTC(y, mo - 1, d))
  return at.getUTCFullYear() === y && at.getUTCMonth() === mo - 1 && at.getUTCDate() === d
}

function toUtc(iso: string): number {
  const [y, m, d] = iso.split('-').map(Number)
  return Date.UTC(y, m - 1, d)
}

function fromUtc(ms: number): string {
  const d = new Date(ms)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`
}

/** Today in Kyiv, as `YYYY-MM-DD`. */
export function kyivToday(now: Date = new Date()): string {
  // en-CA renders a date as YYYY-MM-DD, which is the one shape needed here.
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Europe/Kyiv', year: 'numeric', month: '2-digit', day: '2-digit',
  }).format(now)
}

/** `iso` moved by `days` (negative goes back). */
export function addDays(iso: string, days: number): string {
  return fromUtc(toUtc(iso) + Math.round(days) * DAY_MS)
}

/** Whole days from `from` to `to`; negative when `to` is earlier. */
export function daysBetween(from: string, to: string): number {
  return Math.round((toUtc(to) - toUtc(from)) / DAY_MS)
}

/** How many days ago `iso` was, from `today`. A future date is 0. */
export function daysAgo(iso: string, today: string = kyivToday()): number {
  return Math.max(0, daysBetween(iso, today))
}

/** The date `days` days before `today`. */
export function daysAgoDate(days: number, today: string = kyivToday()): string {
  return addDays(today, -days)
}

/** Days a range covers, both ends included: 1 Jan – 14 Oct 2026 is 287. */
export function inclusiveDays(from: string, to: string): number {
  return daysBetween(from, to) + 1
}

function locale(): string {
  return LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'en-US'
}

type Parts = { day: string; month: string; year: string }

function parts(iso: string): Parts {
  const out: Parts = { day: '', month: '', year: '' }
  for (const p of new Intl.DateTimeFormat(locale(), {
    day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC',
  }).formatToParts(new Date(toUtc(iso)))) {
    if (p.type === 'day') out.day = p.value
    // "янв." and "жовт." carry a period that reads as the end of a sentence
    // in the middle of one.
    if (p.type === 'month') out.month = p.value.replace(/\.$/, '')
    if (p.type === 'year') out.year = p.value
  }
  return out
}

function english(): boolean {
  return locale().startsWith('en')
}

/**
 * One day, said the way people say it: «1 янв 2026», "Jan 1, 2026".
 *
 * Built from Intl's parts rather than taken whole, because Russian and
 * Ukrainian Intl add "г." / "р." after the year — and the month name it gives
 * is already in the genitive, which is the one thing a hand-written table of
 * month names gets wrong.
 */
export function formatDay(iso: string, { withYear = true } = {}): string {
  if (!isIsoDate(iso)) return iso
  const p = parts(iso)
  if (english()) return withYear ? `${p.month} ${p.day}, ${p.year}` : `${p.month} ${p.day}`
  return withYear ? `${p.day} ${p.month} ${p.year}` : `${p.day} ${p.month}`
}

/** A range, the year said once when both ends share it: «1 янв – 14 окт 2026». */
export function formatDayRange(from: string, to: string): string {
  if (!isIsoDate(from) || !isIsoDate(to)) return `${from} – ${to}`
  const sameYear = from.slice(0, 4) === to.slice(0, 4)
  return `${formatDay(from, { withYear: !sameYear })} – ${formatDay(to)}`
}
