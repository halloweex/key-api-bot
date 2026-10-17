import { describe, it, expect } from 'vitest'
import { followUpCandidates, followUpParams } from '../smsFollowUp'
import type { SmsCampaignSummary } from '../../types/api'

const NOW = new Date('2026-10-17T12:00:00Z')

function campaign(
  name: string, sentAt: string | null, followUpOf: string | null = null,
): SmsCampaignSummary {
  return {
    campaign: name, ltvBasis: 'margin', salesType: 'retail', holdoutPct: 10,
    promocode: null, exportedAt: sentAt, sentAt, notes: null,
    members: 10, target: 9, holdout: 1, followUpOf,
  }
}

describe('followUpCandidates', () => {
  it('offers sent campaigns of their own from the last seven days, newest first', () => {
    const picked = followUpCandidates([
      campaign('older', '2026-10-12T09:00:00Z'),
      campaign('today', '2026-10-17T08:00:00Z'),
      campaign('edge', '2026-10-10T12:00:00Z'),          // exactly seven days
      campaign('stale', '2026-10-09T11:59:00Z'),         // a minute past
      campaign('draft', null),                           // never sent
      campaign('noon', '2026-10-17T10:00:00Z', 'today'), // a follow-up itself
    ], NOW)

    expect(picked.map((c) => c.campaign)).toEqual(['today', 'older', 'edge'])
  })

  it('skips a send date it cannot read rather than guessing', () => {
    expect(followUpCandidates([campaign('broken', 'not a date')], NOW)).toEqual([])
  })
})

describe('followUpParams', () => {
  it('carries the campaign it follows and nothing that shapes an audience', () => {
    const p = new URLSearchParams(followUpParams('oct-promo', {
      campaign: 'oct-last-day', promocode: 'LAST',
    }))

    expect(Object.fromEntries(p)).toEqual({
      follow_up_of: 'oct-promo', campaign: 'oct-last-day', promocode: 'LAST',
    })
  })

  it('previews with the root alone', () => {
    expect(followUpParams('oct-promo')).toBe('follow_up_of=oct-promo')
  })
})
