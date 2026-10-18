import { afterEach, describe, it, expect } from 'vitest'
import i18n from '../../lib/i18n'
import { ApiError } from '../../api/client'
import { describeSmsError, refusalMessage, refusalReason } from '../smsRefusal'

// The send's refusals arrive as an English `detail` and, beside it, a
// `reason` with the figures. The owner reads Ukrainian and Russian; these pin
// that what is shown is the reader's sentence with the server's numbers in it.

afterEach(() => { void i18n.changeLanguage('en') })

const t = (key: string, opts?: Record<string, unknown>) => i18n.t(key, opts) as string
// Intl puts no-break and narrow no-break spaces into numbers and dates.
const plain = (s: string | null | undefined) => (s ?? '').replace(/[\u00a0\u202f]/g, ' ')

const refusal = (status: number, body: Record<string, unknown>) =>
  new ApiError(status, String(body.detail ?? 'refused'), 'CLIENT_ERROR', body)

describe('a named refusal', () => {
  it('says a short balance with its money to the kopiyka', async () => {
    await i18n.changeLanguage('ru')
    const e = refusal(409, {
      detail: 'the TurboSMS balance is 4650.00 ₴ …', reason: 'balance_short',
      balance: 4650, cost: 4746.12, shortfall: 96.12, recipients: 3635, parts: 1,
      pricePerPart: 1.28, margin: 1.02,
    })

    expect(plain(refusalMessage(e, t))).toBe(
      'На балансе TurboSMS ₴4 650,00, а рассылка стоит до ₴4 746,12 '
      + '(3 635 получателей × 1 часть × ₴1,28, +2%). Не хватает ₴96,12 — '
      + 'пополните баланс и отправьте снова. Ничего не отправлено.',
    )
  })

  it("names today's other campaigns and the rule each one breaks", async () => {
    await i18n.changeLanguage('uk')
    const e = refusal(409, {
      reason: 'recent_campaign', hours: 24,
      conflicts: [
        { campaign: 'oct-vip', arm: 'control', people: 3, sentAt: '2026-10-17T09:00:00+00:00' },
        { campaign: 'oct-all', arm: 'target', people: 120, sentAt: '2026-10-17T08:00:00+00:00' },
      ],
    })
    const text = plain(refusalMessage(e, t))

    expect(text).toContain('за останні 24 год')
    expect(text).toContain('3 людини — контрольна група oct-vip (надіслано 17 жовт., 12:00)')
    expect(text).toContain('120 людей — отримувачі oct-all')
    expect(text).toContain('Писати контрольній групі іншої кампанії не можна')
    expect(text).toContain('лише як продовження тієї кампанії')
    expect(text.endsWith('Нічого не надіслано.')).toBe(true)
  })

  it('says a mistyped name in the reader’s language', async () => {
    await i18n.changeLanguage('ru')
    const e = refusal(409, {
      detail: "type the campaign name 'sms-2026-10-17' to confirm the send",
      reason: 'confirm_mismatch', campaign: 'sms-2026-10-17',
    })

    expect(refusalMessage(e, t)).toBe(
      'Введите название кампании sms-2026-10-17 точно так, как оно написано.',
    )
  })

  it('has a sentence for every follow-up refusal', async () => {
    await i18n.changeLanguage('ru')
    for (const [reason, figures] of [
      ['follow_up_root_unsent', { root: 'sep' }],
      ['follow_up_of_follow_up', { root: 'noon', parent: 'sep' }],
      ['follow_up_sales_type', { root: 'sep', salesType: 'all' }],
      ['follow_up_self', { root: 'sep' }],
      ['follow_up_holdout', { root: 'sep' }],
      ['follow_up_narrowed', { root: 'sep', params: ['brand', 'tier'] }],
    ] as const) {
      const text = refusalMessage(refusal(409, { reason, ...figures }), t)
      expect(text, reason).toBeTruthy()
      expect(text, reason).not.toMatch(/\{\{|sms\.refusal/)
    }
    expect(refusalMessage(refusal(409, {
      reason: 'follow_up_narrowed', root: 'sep', params: ['brand', 'tier'],
    }), t)).toContain('(brand, tier)')
  })
})

describe('what is not a named refusal', () => {
  it('falls back to the server sentence for a reason with no sentence here', () => {
    const e = refusal(409, { detail: 'a new rule the page does not know', reason: 'brand_new' })

    expect(refusalReason(e)).toBe('brand_new')
    expect(refusalMessage(e, t)).toBeNull()
    expect(describeSmsError(e, t)).toBe('a new rule the page does not know')
  })

  it('falls back to the server sentence when nothing is named', () => {
    const e = new ApiError(400, 'core_ltv must not exceed vip_ltv')

    expect(describeSmsError(e, t)).toBe('core_ltv must not exceed vip_ltv')
  })

  it('says the rate limit itself, whatever the limiter wrote', async () => {
    await i18n.changeLanguage('ru')
    const e = new ApiError(429, 'Too many requests. Please try again later.')

    expect(describeSmsError(e, t)).toBe(
      'Слишком много попыток подряд. Подождите минуту и попробуйте снова.',
    )
  })

  it('leaves an error that is not the API’s alone', () => {
    expect(describeSmsError(new Error('Network down'), t)).toBe('Network down')
    expect(describeSmsError('nope', t)).toBeUndefined()
  })
})
