import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import i18n from '../../lib/i18n'
import { describeChange, describeHistory, diffVersions, type VersionState } from '../promotionDiff'
import { formatDay } from '../smsDates'
import type { T } from '../plural'
import type { PromotionVersion } from '../../types/api'

// Said for what it says: the real dictionaries, in the reader's language,
// with the dates Intl writes in it.
let t: T
beforeEach(async () => {
  await i18n.changeLanguage('uk')
  t = i18n.getFixedT('uk') as T
})
afterEach(() => { void i18n.changeLanguage('en') })

const plain = (s: string) => s.replace(/[\u00a0\u202f]/g, ' ')

const TODAY = '2027-01-10'

function state(over: Partial<VersionState> = {}): VersionState {
  return {
    name: 'Весняний розпродаж', starts_on: '2027-03-05', ends_on: '2027-03-08', start_time: null,
    kind: 'other', discount_pct: 20, scope: 'store', scope_items: [],
    channels: ['sms', 'instagram'], sms_campaign: null, note: null, status: 'planned', ...over,
  }
}

/** What one save changed, in words. */
const said = (prev: VersionState | null, next: VersionState, today = TODAY) =>
  diffVersions(prev, next).map((c) => plain(describeChange(c, t, today)))

describe('what changed between two versions, in words', () => {
  it('says the spec’s three examples as written', () => {
    expect(said(state(), state({ starts_on: '2027-03-06', ends_on: '2027-03-09' })))
      .toEqual(['Перенесено: 5–8 → 6–9 березня'])
    expect(said(state(), state({ discount_pct: 25 }))).toEqual(['Знижка: 20 → 25 %'])
    expect(said(state(), state({ status: 'cancelled' }))).toEqual(['Скасовано'])
  })

  it('says the first version was created and a cancelled one restored', () => {
    expect(said(null, state())).toEqual(['Створено'])
    expect(said(state({ status: 'cancelled' }), state())).toEqual(['Відновлено'])
  })

  it('says the month twice when a move crosses it, and the year when it is not this one', () => {
    expect(said(state(), state({ starts_on: '2027-02-27', ends_on: '2027-03-02' })))
      .toEqual(['Перенесено: 5–8 березня → 27 лютого – 2 березня'])
    expect(said(state(), state({ starts_on: '2027-03-06', ends_on: '2027-03-09' }), '2026-11-19'))
      .toEqual(['Перенесено: 5–8 → 6–9 березня 2027'])
    // One day to another in the same month.
    expect(said(state({ starts_on: '2027-03-05', ends_on: '2027-03-05' }),
      state({ starts_on: '2027-03-06', ends_on: '2027-03-06' })))
      .toEqual(['Перенесено: 5 → 6 березня'])
  })

  it('says a discount set or taken away', () => {
    expect(said(state({ discount_pct: null }), state({ discount_pct: 15 })))
      .toEqual(['Знижка: не вказано → 15 %'])
    expect(said(state({ discount_pct: 12.5 }), state({ discount_pct: null })))
      .toEqual(['Знижка: 12,5 % → не вказано'])
  })

  it('reads a discount the server sends as text as the same number', () => {
    // NUMERIC(5,2) may come back "20.00".
    const asText = state({ discount_pct: '20.00' as unknown as number })
    expect(diffVersions(state(), asText)).toEqual([])
  })

  it('says the name, the hour, the type', () => {
    expect(said(state(), state({ name: 'Весна' })))
      .toEqual(['Назва: «Весняний розпродаж» → «Весна»'])
    expect(said(state(), state({ start_time: '18:00:00' }))).toEqual(['Початок: з ранку → 18:00'])
    expect(said(state({ start_time: '18:00' }), state({ start_time: '18:00:00' }))).toEqual([])
    expect(said(state(), state({ kind: 'black_friday' }))).toEqual(['Тип: інша акція → Чорна п\'ятниця'])
  })

  it('says what a scope became, and the names added to or taken from one', () => {
    expect(said(state(), state({ scope: 'brands', scope_items: ['COSRX', 'Anua'] })))
      .toEqual(['На що: увесь магазин → бренди: COSRX, Anua'])
    expect(said(
      state({ scope: 'brands', scope_items: ['COSRX'] }),
      state({ scope: 'brands', scope_items: ['Anua'] }),
    )).toEqual(['Бренди: +Anua, −COSRX'])
    // Another order of the same names is not a change.
    expect(said(
      state({ scope: 'categories', scope_items: ['Тонери', 'Креми'] }),
      state({ scope: 'categories', scope_items: ['Креми', 'Тонери'] }),
    )).toEqual([])
  })

  it('says the channels added and dropped, by their names', () => {
    expect(said(state(), state({ channels: ['site', 'sms'] })))
      .toEqual(['Канали: +банер на сайті, −Instagram'])
  })

  it('says an SMS campaign linked, changed or unlinked, and a note without quoting it', () => {
    expect(said(state(), state({ sms_campaign: 'nov-bf' }))).toEqual(['Із SMS-кампанії «nov-bf»'])
    expect(said(state({ sms_campaign: 'a' }), state({ sms_campaign: 'b' })))
      .toEqual(['SMS-кампанія: «a» → «b»'])
    expect(said(state({ sms_campaign: 'a' }), state())).toEqual(['Більше не з SMS-кампанії «a»'])
    expect(said(state(), state({ note: 'промокод SPRING' }))).toEqual(['Додано примітку'])
    expect(said(state({ note: 'a' }), state({ note: 'b' }))).toEqual(['Змінено примітку'])
    expect(said(state({ note: 'a' }), state({ note: '  ' }))).toEqual(['Прибрано примітку'])
  })

  it('says every change of one save, the status first', () => {
    expect(said(state(), state({ status: 'cancelled', discount_pct: 30, starts_on: '2027-03-06', ends_on: '2027-03-09' })))
      .toEqual(['Скасовано', 'Перенесено: 5–8 → 6–9 березня', 'Знижка: 20 → 30 %'])
  })

  it('in English, the month on both sides of a move', async () => {
    await i18n.changeLanguage('en')
    const en = i18n.getFixedT('en') as T
    const [move] = diffVersions(state(), state({ starts_on: '2027-03-06', ends_on: '2027-03-09' }))
    expect(describeChange(move, en, TODAY)).toBe('Moved: March 5–8 → March 6–9')
    const [discount] = diffVersions(state(), state({ discount_pct: 25 }))
    expect(describeChange(discount, en, TODAY)).toBe('Discount: 20 → 25%')
  })
})

describe('a promotion’s history', () => {
  function version(v: number, over: Partial<PromotionVersion>): PromotionVersion {
    return {
      ...state(), id: 12, version: v, source: 'form',
      recorded_at: '2026-11-20T09:01:44+02:00', recorded_by_me: false, ...over,
    }
  }

  it('says each version against the one before it, newest first, and whose it was only if it was the reader’s', () => {
    const versions = [
      version(3, { status: 'cancelled', discount_pct: 25, recorded_at: '2026-11-22T12:00:00+02:00' }),
      version(2, { discount_pct: 25, recorded_by_me: true, recorded_at: '2026-11-21T08:30:00+02:00' }),
      version(1, { recorded_at: '2026-11-19T10:12:03+02:00' }),
    ]
    const entries = describeHistory(versions, t, '2026-11-25')
    expect(entries.map((e) => [e.version, e.byMe, e.changes.map(plain)])).toEqual([
      [3, false, ['Скасовано']],
      [2, true, ['Знижка: 20 → 25 %']],
      [1, false, ['Створено']],
    ])
    expect(entries[2].when).toBe(`${formatDay('2026-11-19', { withYear: false })}, 10:12`)
  })

  it('says a save that changed nothing rather than nothing', () => {
    const entries = describeHistory([version(2, {}), version(1, {})], t, '2026-11-25')
    expect(entries[0].changes).toEqual(['Збережено без змін'])
  })
})
