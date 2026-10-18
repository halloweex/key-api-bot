/**
 * An SMS campaign refusal, said in the reader's language.
 *
 * The campaign routes answer a refusal with an English `detail` — what an API
 * caller reads — and, beside it, a `reason` and the figures behind it
 * (`core/sms_refusals.py`). The page used to show `detail` as it came, so the
 * owner, who works in Ukrainian and Russian, read in English that the balance
 * was short or that these people had been messaged today.
 *
 * `sms.refusal.<reason>` is the sentence, filled in with the figures formatted
 * for the reader: money to the kopiyka (a shortfall of ₴3.40 is not "₴3"),
 * people as counted words, instants in Kyiv time. A reason this page has no
 * sentence for falls back to `detail`, never to a raw key.
 */
import { ApiError } from '../api/client'
import i18n, { LANGUAGE_LOCALES, type SupportedLanguage } from '../lib/i18n'
import { formatNumber } from './formatters'
import { plural, type T } from './smsConditionText'

type Figures = Record<string, unknown>

function locale(): string {
  return LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'en-US'
}

const num = (v: unknown): number | null => (typeof v === 'number' && Number.isFinite(v) ? v : null)
const str = (v: unknown): string => (typeof v === 'string' ? v : '')

function money(v: unknown): string {
  const n = num(v)
  if (n == null) return '—'
  return `₴${new Intl.NumberFormat(locale(), {
    minimumFractionDigits: 2, maximumFractionDigits: 2,
  }).format(n)}`
}

/** «17 окт., 11:05» — when something was sent, on the shop's clock. */
function when(v: unknown): string {
  const at = new Date(str(v))
  if (Number.isNaN(at.getTime())) return str(v)
  return new Intl.DateTimeFormat(locale(), {
    day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit',
    timeZone: 'Europe/Kyiv',
  }).format(at)
}

/** The figures of one refusal, as its sentence uses them. */
function fill(body: Figures, t: T): Record<string, string | number> {
  const out: Record<string, string | number> = {}
  for (const key of ['campaign', 'root', 'parent', 'salesType', 'error']) {
    out[key] = str(body[key])
  }
  out.sentAt = when(body.sentAt)
  out.hours = num(body.hours) ?? 24
  out.limit = formatNumber(num(body.limit) ?? 0)
  out.params = Array.isArray(body.params) ? body.params.map(String).join(', ') : ''
  for (const key of ['balance', 'cost', 'shortfall']) out[key] = money(body[key])
  out.price = money(body.pricePerPart)
  const margin = num(body.margin)
  out.margin = margin == null ? 2 : Math.round((margin - 1) * 100)
  const recipients = num(body.recipients) ?? 0
  out.recipients = `${formatNumber(recipients)} ${plural(t, 'word.recipients', recipients)}`
  const parts = num(body.parts) ?? 1
  out.parts = `${parts} ${plural(t, 'word.parts', parts)}`

  // Who else was messaged today, and why that stops this send — one sentence
  // per arm, because the two are different rules.
  const conflicts = Array.isArray(body.conflicts) ? body.conflicts as Figures[] : []
  const armOf = (c: Figures) => (c.arm === 'control' ? 'control' : 'target')
  out.conflicts = conflicts.map((c) => {
    const people = num(c.people) ?? 0
    return t('sms.refusalConflict', {
      people: formatNumber(people), word: plural(t, 'word.people', people),
      arm: t(`sms.refusalArm.${armOf(c)}`), campaign: str(c.campaign), sentAt: when(c.sentAt),
    })
  }).join('; ')
  out.why = (['control', 'target'] as const)
    .filter((arm) => conflicts.some((c) => armOf(c) === arm))
    .map((arm) => t(`sms.refusalWhy.${arm}`))
    .join(' ')
  return out
}

/** The refusal's name, when the server gave one. */
export function refusalReason(error: unknown): string | null {
  if (!(error instanceof ApiError)) return null
  const reason = error.body?.reason
  return typeof reason === 'string' && reason ? reason : null
}

/**
 * The refusal in the reader's language, or null when there is nothing to
 * translate — no reason, or one this page has no sentence for.
 */
export function refusalMessage(error: unknown, t: T): string | null {
  if (!(error instanceof ApiError)) return null
  // The limiter answers in its own words and names nothing; the status is
  // the whole message.
  if (error.status === 429) return t('sms.refusalRateLimited')
  const reason = refusalReason(error)
  if (!reason || !i18n.exists(`sms.refusal.${reason}`)) return null
  return t(`sms.refusal.${reason}`, fill(error.body ?? {}, t))
}

/** What to show for any error a campaign route answered: the translated
 *  refusal, else the server's own sentence. */
export function describeSmsError(error: unknown, t: T): string | undefined {
  return refusalMessage(error, t) ?? (error instanceof Error ? error.message : undefined)
}
