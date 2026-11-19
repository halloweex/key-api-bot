import { afterEach, describe, expect, it } from 'vitest'
import i18n from '../../lib/i18n'
import {
  draftFromPromotion,
  draftToPayload,
  emptyDraft,
  formatRecordedAt,
  isOngoing,
  promotionDays,
  splitPromotions,
  validateDraft,
  type PromotionDraft,
} from '../promotions'
import { formatDay } from '../smsDates'
import type { Promotion } from '../../types/api'

afterEach(() => { void i18n.changeLanguage('en') })

// Intl writes a no-break space here and there; the reader sees a space.
const plain = (s: string) => s.replace(/[\u00a0\u202f]/g, ' ')

function draft(over: Partial<PromotionDraft> = {}): PromotionDraft {
  return {
    ...emptyDraft(),
    name: 'Весняний розпродаж', startsOn: '2027-03-05', endsOn: '2027-03-08', ...over,
  }
}

function promotion(over: Partial<Promotion> = {}): Promotion {
  return {
    id: 12, version: 3, name: 'Весняний розпродаж',
    starts_on: '2027-03-05', ends_on: '2027-03-08', start_time: null,
    kind: 'other', discount_pct: 20, scope: 'brands', scope_items: ['COSRX'],
    channels: ['sms', 'instagram'], sms_campaign: null, note: null,
    status: 'planned', created_at: '2026-11-19T10:12:03+02:00',
    updated_at: '2026-11-20T09:01:44+02:00', source: 'form', ...over,
  }
}

// ─── The form refuses what the API refuses ─────────────────────────────────

describe('validating a promotion before it is sent', () => {
  it('lets a filled form through', () => {
    expect(validateDraft(draft())).toEqual({})
  })

  it('wants a name of 1–120 characters, spaces not counted', () => {
    expect(validateDraft(draft({ name: '   ' })).name?.key).toBe('promo.error.nameRequired')
    expect(validateDraft(draft({ name: 'x'.repeat(121) })).name)
      .toEqual({ key: 'promo.error.tooLong', params: { max: 120 } })
    expect(validateDraft(draft({ name: ` ${'x'.repeat(120)} ` })).name).toBeUndefined()
  })

  it('wants both days, the last not before the first', () => {
    const empty = validateDraft(draft({ startsOn: '', endsOn: '' }))
    expect(empty.startsOn?.key).toBe('promo.error.dateRequired')
    expect(empty.endsOn?.key).toBe('promo.error.dateRequired')
    // What a native date field holds while its year is being typed.
    expect(validateDraft(draft({ startsOn: '0002-03-05' })).startsOn?.key).toBe('promo.error.dateRequired')
    expect(validateDraft(draft({ endsOn: '2027-03-04' })).endsOn?.key).toBe('promo.error.endBeforeStart')
    expect(validateDraft(draft({ endsOn: '2027-03-05' })).endsOn).toBeUndefined()
  })

  it('holds a promotion to 62 days, both ends counted', () => {
    // 1 March + 61 days is 1 May: 62 days in all.
    expect(validateDraft(draft({ startsOn: '2027-03-01', endsOn: '2027-05-01' })).endsOn).toBeUndefined()
    expect(validateDraft(draft({ startsOn: '2027-03-01', endsOn: '2027-05-02' })).endsOn)
      .toEqual({ key: 'promo.error.spanTooLong', params: { max: 62 } })
  })

  it('takes a start time as HH:MM, or none for the morning', () => {
    expect(validateDraft(draft({ startTime: '' })).startTime).toBeUndefined()
    expect(validateDraft(draft({ startTime: '18:00' })).startTime).toBeUndefined()
    expect(validateDraft(draft({ startTime: '24:00' })).startTime?.key).toBe('promo.error.startTime')
    expect(validateDraft(draft({ startTime: '9:00' })).startTime?.key).toBe('promo.error.startTime')
  })

  it('takes a discount from 0 to 90 with at most two decimals, or none', () => {
    for (const ok of ['', '0', '20', '12.5', '89.99', '90', '12,5']) {
      expect(validateDraft(draft({ discount: ok })).discount, ok).toBeUndefined()
    }
    for (const bad of ['-5', '90.01', '91', '12.345', '1e2', 'abc']) {
      expect(validateDraft(draft({ discount: bad })).discount, bad)
        .toEqual({ key: 'promo.error.discount', params: { max: 90 } })
    }
  })

  it('wants brands or categories named once the scope is narrower than the store', () => {
    expect(validateDraft(draft({ scope: 'store', scopeItems: [] })).scopeItems).toBeUndefined()
    expect(validateDraft(draft({ scope: 'brands', scopeItems: [] })).scopeItems?.key)
      .toBe('promo.error.scopeItemsRequired')
    expect(validateDraft(draft({ scope: 'categories', scopeItems: ['  '] })).scopeItems?.key)
      .toBe('promo.error.scopeItemsRequired')
    const many = Array.from({ length: 51 }, (_, i) => `brand ${i}`)
    expect(validateDraft(draft({ scope: 'brands', scopeItems: many })).scopeItems)
      .toEqual({ key: 'promo.error.scopeItemsTooMany', params: { max: 50 } })
    expect(validateDraft(draft({ scope: 'brands', scopeItems: ['x'.repeat(81)] })).scopeItems)
      .toEqual({ key: 'promo.error.scopeItemTooLong', params: { max: 80 } })
  })

  it('holds a note to 500 characters', () => {
    expect(validateDraft(draft({ note: 'x'.repeat(500) })).note).toBeUndefined()
    expect(validateDraft(draft({ note: 'x'.repeat(501) })).note)
      .toEqual({ key: 'promo.error.tooLong', params: { max: 500 } })
  })
})

// ─── What is sent ──────────────────────────────────────────────────────────

describe('the body sent to /api/promotions', () => {
  it('is the contract’s PromotionIn, nothing said left null', () => {
    expect(draftToPayload(draft())).toEqual({
      name: 'Весняний розпродаж', starts_on: '2027-03-05', ends_on: '2027-03-08',
      start_time: null, kind: 'other', discount_pct: null, scope: 'store',
      scope_items: [], channels: [], sms_campaign: null, note: null, source: 'form',
    })
  })

  it('sends numbers as numbers, trimmed text, channels in one order', () => {
    expect(draftToPayload(draft({
      name: '  Весна  ', startTime: '18:00', discount: '12,5', scope: 'brands',
      scopeItems: [' COSRX ', 'Anua', ' '], channels: ['site', 'sms'], note: ' промокод SPRING ',
    }))).toMatchObject({
      name: 'Весна', start_time: '18:00', discount_pct: 12.5,
      scope_items: ['COSRX', 'Anua'], channels: ['sms', 'site'], note: 'промокод SPRING',
    })
  })

  it('sends no names for the whole store, whatever the form still holds', () => {
    expect(draftToPayload(draft({ scope: 'store', scopeItems: ['COSRX'] })).scope_items).toEqual([])
  })

  it('says the version came from an SMS campaign only when it was picked here', () => {
    expect(draftToPayload(draft({ smsCampaign: 'nov-bf', fromSms: true })))
      .toMatchObject({ sms_campaign: 'nov-bf', source: 'form_from_sms' })
    expect(draftToPayload(draft({ smsCampaign: 'nov-bf', fromSms: false })))
      .toMatchObject({ sms_campaign: 'nov-bf', source: 'form' })
  })

  it('opens a promotion as it stands, ready to be saved as its next version', () => {
    const d = draftFromPromotion(promotion({ start_time: '18:00:00', discount_pct: 20, note: 'x' }))
    expect(d).toMatchObject({
      startTime: '18:00', discount: '20', scope: 'brands', scopeItems: ['COSRX'],
      channels: ['sms', 'instagram'], note: 'x', fromSms: false,
    })
    expect(validateDraft(d)).toEqual({})
    expect(draftFromPromotion(promotion({ discount_pct: null })).discount).toBe('')
  })
})

// ─── The page's two lists ──────────────────────────────────────────────────

describe('upcoming and past', () => {
  const today = '2026-11-19'
  const list = [
    promotion({ id: 1, starts_on: '2026-08-01', ends_on: '2026-08-03' }), // over 90 days ago
    promotion({ id: 2, starts_on: '2026-09-10', ends_on: '2026-09-12' }),
    promotion({ id: 3, starts_on: '2026-10-01', ends_on: '2026-10-02', status: 'cancelled' }),
    promotion({ id: 4, starts_on: '2026-11-10', ends_on: '2026-11-11' }),
    promotion({ id: 5, starts_on: '2026-11-17', ends_on: '2026-11-19' }), // ends today
    promotion({ id: 6, starts_on: '2026-11-27', ends_on: '2026-11-30', kind: 'black_friday' }),
    promotion({ id: 7, starts_on: '2026-12-20', ends_on: '2026-12-24', status: 'cancelled' }),
  ]
  const ids = (ps: Promotion[]) => ps.map((p) => p.id)

  it('puts what ends today or later ahead, soonest first', () => {
    expect(ids(splitPromotions(list, today, false).upcoming)).toEqual([5, 6])
  })

  it('puts the last 90 days behind, latest first', () => {
    expect(ids(splitPromotions(list, today, false).past)).toEqual([4, 2])
  })

  it('shows the cancelled ones only when asked, and counts them either way', () => {
    const hidden = splitPromotions(list, today, false)
    const shown = splitPromotions(list, today, true)
    expect(hidden.cancelled).toBe(2)
    expect(ids(shown.upcoming)).toEqual([5, 6, 7])
    expect(ids(shown.past)).toEqual([4, 3, 2])
  })

  it('knows a promotion running today', () => {
    expect(isOngoing(list[4], today)).toBe(true)
    expect(isOngoing(list[5], today)).toBe(false)
  })
})

describe('saying a promotion’s days', () => {
  it('leaves this year out and says another', async () => {
    await i18n.changeLanguage('uk')
    expect(plain(promotionDays('2026-11-26', '2026-11-28', '2026-11-19'))).toBe('26–28 листопада')
    expect(plain(promotionDays('2026-11-30', '2026-12-02', '2026-11-19'))).toBe('30 листопада – 2 грудня')
    expect(plain(promotionDays('2027-03-05', '2027-03-08', '2026-11-19'))).toBe('5–8 березня 2027')
    expect(plain(promotionDays('2026-12-30', '2027-01-02', '2026-11-19')))
      .toBe('30 грудня 2026 – 2 січня 2027')
    expect(plain(promotionDays('2026-11-28', '2026-11-28', '2026-11-19'))).toBe('28 листопада')
  })

  it('in English, the month first', () => {
    expect(promotionDays('2026-11-26', '2026-11-28', '2026-11-19')).toBe('November 26–28')
  })

  it('says when a version was saved in Kyiv', async () => {
    await i18n.changeLanguage('uk')
    // 07:01 UTC is 09:01 in Kyiv in November.
    expect(formatRecordedAt('2026-11-20T07:01:44Z', '2026-11-25'))
      .toBe(`${formatDay('2026-11-20', { withYear: false })}, 09:01`)
    // 22:30 UTC on 30 December is half past midnight on the 31st in Kyiv,
    // a year that is not this one, so it is said.
    expect(formatRecordedAt('2025-12-30T22:30:00Z', '2026-11-25'))
      .toBe(`${formatDay('2025-12-31')}, 00:30`)
  })
})
