/**
 * The promotions calendar's model: the form's draft, its validation, the body
 * it sends, and how the page splits and says the list.
 *
 * Validation repeats what `/api/promotions` refuses (`PromotionIn`), so a
 * person sees the problem beside its field before anything is sent. The
 * server stays the authority: whatever it still refuses is shown as its own
 * `detail`, in a toast.
 */
import type {
  Promotion,
  PromotionChannel,
  PromotionIn,
  PromotionKind,
  PromotionScope,
} from '../types/api'
import i18n, { LANGUAGE_LOCALES, type SupportedLanguage } from '../lib/i18n'
import { addDays, formatDay, formatSpan, inclusiveDays, isIsoDate, kyivToday } from './smsDates'

export const PROMOTION_KINDS: PromotionKind[] = ['other', 'black_friday']
export const PROMOTION_SCOPES: PromotionScope[] = ['store', 'brands', 'categories']
/** In this order on the form and in a sentence. */
export const PROMOTION_CHANNELS: PromotionChannel[] = ['sms', 'instagram', 'telegram', 'site']

/** The API's bounds (`app.promotion_versions`' CHECKs and `PromotionIn`). */
export const LIMITS = {
  name: 120,
  note: 500,
  scopeItems: 50,
  scopeItem: 80,
  discount: 90,
  /** Days a promotion may span, both ends included: two months. */
  spanDays: 62,
} as const

/** How far back the "past" list reaches, and the server's default window. */
export const PAST_DAYS = 90
/**
 * How far ahead a promotion may start: the list reads up to today + this, so
 * a later one would be saved and then shown nowhere, to be changed or
 * cancelled. The API refuses it too (`starts_on`).
 */
export const AHEAD_DAYS = 365

/** The last first day a promotion may have, in Kyiv. */
export function latestStart(today: string = kyivToday()): string {
  return addDays(today, AHEAD_DAYS)
}

// ─── The form's draft ────────────────────────────────────────────────────────

/** What the form holds while it is being filled: text as typed. */
export interface PromotionDraft {
  name: string
  /** `YYYY-MM-DD`, or '' while not picked. */
  startsOn: string
  endsOn: string
  /** `HH:MM`, or '' for a start in the morning. */
  startTime: string
  kind: PromotionKind
  /** As typed; '' when not said. */
  discount: string
  scope: PromotionScope
  scopeItems: string[]
  channels: PromotionChannel[]
  smsCampaign: string | null
  note: string
  /** The SMS campaign was picked in this form: the version's `source`. */
  fromSms: boolean
}

export function emptyDraft(): PromotionDraft {
  return {
    name: '', startsOn: '', endsOn: '', startTime: '', kind: 'other', discount: '',
    scope: 'store', scopeItems: [], channels: [], smsCampaign: null, note: '', fromSms: false,
  }
}

/** A number as the form shows it: `20`, `12.5`, never `20.00`. */
function discountText(value: number | string | null): string {
  if (value === null || value === undefined || value === '') return ''
  const n = Number(value)
  return Number.isFinite(n) ? String(n) : ''
}

/** The form opened on a promotion: its latest version, to be saved as the next. */
export function draftFromPromotion(p: Promotion): PromotionDraft {
  return {
    name: p.name,
    startsOn: p.starts_on,
    endsOn: p.ends_on,
    // A TIME column may come back with seconds.
    startTime: p.start_time ? p.start_time.slice(0, 5) : '',
    kind: p.kind,
    discount: discountText(p.discount_pct),
    scope: p.scope,
    scopeItems: [...p.scope_items],
    channels: [...p.channels],
    smsCampaign: p.sms_campaign,
    note: p.note ?? '',
    fromSms: false,
  }
}

// ─── Validation ──────────────────────────────────────────────────────────────

export type DraftField =
  | 'name' | 'startsOn' | 'endsOn' | 'startTime' | 'discount' | 'scopeItems' | 'note'

/** An i18n key under `promo.error.` and what it says. */
export interface DraftError {
  key: string
  params?: Record<string, number | string>
}

export type DraftErrors = Partial<Record<DraftField, DraftError>>

const TIME = /^([01]\d|2[0-3]):[0-5]\d$/
const DISCOUNT = /^\d+(\.\d{1,2})?$/

/** What is wrong with the draft, field by field; `{}` when it can be sent.
 *  `today` is Kyiv's, the day the first day is counted from. */
export function validateDraft(d: PromotionDraft, today: string = kyivToday()): DraftErrors {
  const errors: DraftErrors = {}
  const name = d.name.trim()
  if (!name) errors.name = { key: 'promo.error.nameRequired' }
  else if (name.length > LIMITS.name) errors.name = { key: 'promo.error.tooLong', params: { max: LIMITS.name } }

  const startOk = isIsoDate(d.startsOn)
  const endOk = isIsoDate(d.endsOn)
  const latest = latestStart(today)
  if (!startOk) errors.startsOn = { key: 'promo.error.dateRequired' }
  else if (d.startsOn > latest) errors.startsOn = { key: 'promo.error.startTooFar', params: { date: formatDay(latest) } }
  if (!endOk) errors.endsOn = { key: 'promo.error.dateRequired' }
  if (startOk && endOk) {
    if (d.endsOn < d.startsOn) errors.endsOn = { key: 'promo.error.endBeforeStart' }
    else if (inclusiveDays(d.startsOn, d.endsOn) > LIMITS.spanDays) {
      errors.endsOn = { key: 'promo.error.spanTooLong', params: { max: LIMITS.spanDays } }
    }
  }

  if (d.startTime && !TIME.test(d.startTime)) errors.startTime = { key: 'promo.error.startTime' }

  const discount = d.discount.trim().replace(',', '.')
  if (discount && (!DISCOUNT.test(discount) || Number(discount) > LIMITS.discount)) {
    errors.discount = { key: 'promo.error.discount', params: { max: LIMITS.discount } }
  }

  if (d.scope !== 'store') {
    const items = d.scopeItems.map((s) => s.trim()).filter(Boolean)
    if (!items.length) errors.scopeItems = { key: 'promo.error.scopeItemsRequired' }
    else if (items.length > LIMITS.scopeItems) {
      errors.scopeItems = { key: 'promo.error.scopeItemsTooMany', params: { max: LIMITS.scopeItems } }
    } else if (items.some((s) => s.length > LIMITS.scopeItem)) {
      errors.scopeItems = { key: 'promo.error.scopeItemTooLong', params: { max: LIMITS.scopeItem } }
    }
  }

  if (d.note.trim().length > LIMITS.note) {
    errors.note = { key: 'promo.error.tooLong', params: { max: LIMITS.note } }
  }
  return errors
}

/** The body of `POST`/`PUT /api/promotions` from a draft that validated. */
export function draftToPayload(d: PromotionDraft): PromotionIn {
  const discount = d.discount.trim().replace(',', '.')
  return {
    name: d.name.trim(),
    starts_on: d.startsOn,
    ends_on: d.endsOn,
    start_time: d.startTime || null,
    kind: d.kind,
    discount_pct: discount === '' ? null : Number(discount),
    scope: d.scope,
    // The whole store names nothing; items of another scope stay behind.
    scope_items: d.scope === 'store' ? [] : d.scopeItems.map((s) => s.trim()).filter(Boolean),
    channels: PROMOTION_CHANNELS.filter((c) => d.channels.includes(c)),
    sms_campaign: d.smsCampaign || null,
    note: d.note.trim() || null,
    source: d.fromSms ? 'form_from_sms' : 'form',
  }
}

// ─── The page's two lists ────────────────────────────────────────────────────

/**
 * Upcoming: what ends today or later (ongoing included), soonest first.
 * Past: what ended in the last `PAST_DAYS` days, latest first. Cancelled
 * ones only when asked for. The server orders by `starts_on`, then `id`.
 */
export function splitPromotions(
  list: Promotion[], today: string, showCancelled: boolean,
): { upcoming: Promotion[]; past: Promotion[]; cancelled: number } {
  const shown = list.filter((p) => showCancelled || p.status === 'planned')
  const since = addDays(today, -PAST_DAYS)
  return {
    upcoming: shown.filter((p) => p.ends_on >= today),
    past: shown.filter((p) => p.ends_on < today && p.ends_on >= since).reverse(),
    cancelled: list.filter((p) => p.status === 'cancelled').length,
  }
}

/** Is it running today? */
export function isOngoing(p: { starts_on: string; ends_on: string }, today: string): boolean {
  return p.starts_on <= today && today <= p.ends_on
}

// ─── Saying it ───────────────────────────────────────────────────────────────

/** A promotion's days, the year said only when they are not this year's:
 *  «26–28 листопада», «30 грудня 2026 – 2 січня 2027». */
export function promotionDays(startsOn: string, endsOn: string, today: string = kyivToday()): string {
  const year = today.slice(0, 4)
  const thisYear = startsOn.slice(0, 4) === year && endsOn.slice(0, 4) === year
  return formatSpan(startsOn, endsOn, { withYear: !thisYear })
}

function locale(): string {
  return LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'en-US'
}

/** `20`, `12,5` in Ukrainian: the reader's decimal mark, no trailing zeros. */
export function formatDiscount(value: number | string): string {
  return new Intl.NumberFormat(locale(), { maximumFractionDigits: 2 }).format(Number(value))
}

/** When a version was saved, in Kyiv: «20 лист, 09:01», the year only when
 *  it is not this one. */
export function formatRecordedAt(iso: string, today: string = kyivToday()): string {
  const at = new Date(iso)
  if (Number.isNaN(at.getTime())) return iso
  const day = kyivToday(at)
  const time = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/Kyiv', hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).format(at)
  return `${formatDay(day, { withYear: day.slice(0, 4) !== today.slice(0, 4) })}, ${time}`
}
