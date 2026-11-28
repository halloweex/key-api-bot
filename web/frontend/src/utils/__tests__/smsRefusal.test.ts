import { afterEach, describe, it, expect } from 'vitest'
import i18n from '../../lib/i18n'
import { ApiError } from '../../api/client'
import { describeSmsError, refusalMessage, refusalReason } from '../smsRefusal'

// The send's refusals arrive as an English `detail` and, beside it, a
// `reason` with the figures. The owner reads Ukrainian and Russian; these pin
// that what is shown is the reader's sentence with the server's numbers in it.

afterEach(() => { void i18n.changeLanguage('en') })

// The decision is which sentence (its key) and which figures go into it; the
// sentences themselves are the locale files' business. A recording `t` returns
// the key and keeps what it was given.
type Call = { key: string; opts?: Record<string, unknown> }
function recorder() {
  const calls: Call[] = []
  const t = (key: string, opts?: Record<string, unknown>) => {
    calls.push({ key, opts })
    return key
  }
  const call = (key: string) => calls.find((c) => c.key === key)
  return { t, calls, call }
}
// Intl puts no-break and narrow no-break spaces into numbers.
const plain = (s: unknown) => String(s ?? '').replace(/[\u00a0\u202f]/g, ' ')

const refusal = (status: number, body: Record<string, unknown>) =>
  new ApiError(status, String(body.detail ?? 'refused'), 'CLIENT_ERROR', body)

describe('a named refusal', () => {
  it('picks the short-balance sentence and fills it with the body’s money, to the kopiyka', () => {
    const { t, call } = recorder()
    const e = refusal(409, {
      detail: 'the TurboSMS balance is 4650.00 ₴ …', reason: 'balance_short',
      balance: 4650, cost: 4746.12, shortfall: 96.12, recipients: 3635, parts: 1,
      pricePerPart: 1.28, margin: 1.02,
    })

    expect(refusalMessage(e, t)).toBe('sms.refusal.balance_short')
    const opts = call('sms.refusal.balance_short')!.opts!
    expect(plain(opts.balance)).toBe('₴4,650.00')
    expect(plain(opts.cost)).toBe('₴4,746.12')
    expect(plain(opts.shortfall)).toBe('₴96.12')
    expect(plain(opts.price)).toBe('₴1.28')
    expect(opts.margin).toBe(2)               // 1.02 → "+2%"
    expect(plain(opts.recipients)).toMatch(/^3,635 /)
    expect(opts.parts).toMatch(/^1 /)
  })

  it('names each of today’s other campaigns, its arm and its time in Kyiv', async () => {
    await i18n.changeLanguage('uk')           // 24-hour clock
    const { t, calls, call } = recorder()
    const e = refusal(409, {
      reason: 'recent_campaign', hours: 24,
      conflicts: [
        { campaign: 'oct-vip', arm: 'control', people: 3, sentAt: '2026-10-17T09:00:00+00:00' },
        { campaign: 'oct-all', arm: 'target', people: 120, sentAt: '2026-10-17T08:00:00+00:00' },
      ],
    })

    expect(refusalMessage(e, t)).toBe('sms.refusal.recent_campaign')
    const rows = calls.filter((c) => c.key === 'sms.refusalConflict').map((c) => c.opts!)
    expect(rows.map((r) => [r.campaign, r.arm, plain(r.people)]))
      .toEqual([['oct-vip', 'sms.refusalArm.control', '3'], ['oct-all', 'sms.refusalArm.target', '120']])
    // 09:00 UTC is 12:00 in Kyiv in October.
    expect(plain(rows[0].sentAt)).toContain('12:00')
    expect(plain(rows[1].sentAt)).toContain('11:00')
    // A rule for each arm that was messaged, said once.
    expect(call('sms.refusalWhy.control')).toBeDefined()
    expect(call('sms.refusalWhy.target')).toBeDefined()
    expect(call('sms.refusal.recent_campaign')!.opts!.hours).toBe(24)
  })

  it('hands a mistyped name back to the sentence unchanged', () => {
    const { t, call } = recorder()
    const e = refusal(409, {
      detail: "type the campaign name 'sms-2026-10-17' to confirm the send",
      reason: 'confirm_mismatch', campaign: 'sms-2026-10-17',
    })

    expect(refusalMessage(e, t)).toBe('sms.refusal.confirm_mismatch')
    expect(call('sms.refusal.confirm_mismatch')!.opts!.campaign).toBe('sms-2026-10-17')
  })

  it('has a sentence for every follow-up refusal', () => {
    for (const [reason, figures] of [
      ['follow_up_root_unsent', { root: 'sep' }],
      ['follow_up_of_follow_up', { root: 'noon', parent: 'sep' }],
      ['follow_up_sales_type', { root: 'sep', salesType: 'all' }],
      ['follow_up_self', { root: 'sep' }],
      ['follow_up_holdout', { root: 'sep' }],
      ['follow_up_narrowed', { root: 'sep', params: ['brand', 'tier'] }],
    ] as const) {
      const { t } = recorder()
      // A reason with no sentence in the locales would come back as null.
      expect(refusalMessage(refusal(409, { reason, ...figures }), t), reason)
        .toBe(`sms.refusal.${reason}`)
    }
    const { t, call } = recorder()
    refusalMessage(refusal(409, {
      reason: 'follow_up_narrowed', root: 'sep', params: ['brand', 'tier'],
    }), t)
    expect(call('sms.refusal.follow_up_narrowed')!.opts!.params).toBe('brand, tier')
  })
})

describe('what is not a named refusal', () => {
  it('falls back to the server sentence for a reason with no sentence here', () => {
    const { t } = recorder()
    const e = refusal(409, { detail: 'a new rule the page does not know', reason: 'brand_new' })

    expect(refusalReason(e)).toBe('brand_new')
    expect(refusalMessage(e, t)).toBeNull()
    expect(describeSmsError(e, t)).toBe('a new rule the page does not know')
  })

  it('falls back to the server sentence when nothing is named', () => {
    const { t } = recorder()
    const e = new ApiError(400, 'core_ltv must not exceed vip_ltv')

    expect(describeSmsError(e, t)).toBe('core_ltv must not exceed vip_ltv')
  })

  it('says the rate limit itself, whatever the limiter wrote', () => {
    const { t } = recorder()
    const e = new ApiError(429, 'Too many requests. Please try again later.')

    expect(describeSmsError(e, t)).toBe('sms.refusalRateLimited')
  })

  it('leaves an error that is not the API’s alone', () => {
    const { t } = recorder()
    expect(describeSmsError(new Error('Network down'), t)).toBe('Network down')
    expect(describeSmsError('nope', t)).toBeUndefined()
  })
})
