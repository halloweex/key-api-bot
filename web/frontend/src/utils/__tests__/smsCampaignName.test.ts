import { describe, expect, it } from 'vitest'
import { CAMPAIGN_NAME_MAX, suggestCampaignName } from '../smsCampaignName'

// The server's own rule for a campaign name (`_CAMPAIGN_PATTERN`).
const VALID = /^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$/
const OCT_17 = new Date(2026, 9, 17, 14, 30)

describe('suggestCampaignName', () => {
  it('names a new campaign after the day it is built', () => {
    expect(suggestCampaignName([], { now: OCT_17 })).toBe('sms-2026-10-17')
  })

  it('pads the month and the day', () => {
    expect(suggestCampaignName([], { now: new Date(2026, 0, 5) })).toBe('sms-2026-01-05')
  })

  it('never offers a name the list already holds, whatever its case', () => {
    expect(suggestCampaignName(['SMS-2026-10-17'], { now: OCT_17 })).toBe('sms-2026-10-17-2')
    expect(
      suggestCampaignName(['sms-2026-10-17', 'sms-2026-10-17-2'], { now: OCT_17 }),
    ).toBe('sms-2026-10-17-3')
  })

  it('names a follow-up after the campaign it follows', () => {
    expect(suggestCampaignName(['aug-summer-sale'], { followUpOf: 'aug-summer-sale' }))
      .toBe('aug-summer-sale-push')
    expect(
      suggestCampaignName(['aug-summer-sale', 'aug-summer-sale-push'],
        { followUpOf: 'aug-summer-sale' }),
    ).toBe('aug-summer-sale-push-2')
  })

  it('fits the server limit when the followed name is already long', () => {
    const root = 'a'.repeat(38)
    const first = suggestCampaignName([root], { followUpOf: root })
    const second = suggestCampaignName([root, first], { followUpOf: root })
    for (const name of [first, second]) {
      expect(name.length).toBeLessThanOrEqual(CAMPAIGN_NAME_MAX)
      expect(name).toMatch(VALID)
    }
    expect(second).not.toBe(first)
  })

  it('does not leave a dangling dash where a long name was cut', () => {
    const root = `${'a'.repeat(35)}-bbbb`
    const name = suggestCampaignName([root], { followUpOf: root })
    expect(name).toMatch(VALID)
    expect(name.endsWith('-')).toBe(false)
  })
})
