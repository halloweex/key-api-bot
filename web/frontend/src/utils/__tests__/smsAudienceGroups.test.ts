import { describe, it, expect } from 'vitest'
import {
  audienceFromPreset,
  audienceToParams,
  audienceToPreset,
  emptyAudience,
  emptyGroup,
  MAX_AUDIENCE_GROUPS,
} from '../smsAudience'
import type { SmsAudienceCriteria } from '../../types/api'

// The owner's campaign: two or more purchases since 1 January, OR anybody who
// bought in the last 92 days.
const OWNER: SmsAudienceCriteria = {
  ...emptyAudience(),
  groups: [
    { maxRecencyDays: 365, filters: { periodOrdersMin: 2, periodFrom: '2026-01-01' } },
    { maxRecencyDays: 92, filters: {} },
  ],
}

describe('groups on the wire', () => {
  it('sends one group exactly as before groups existed', () => {
    // Same parameters, same order — a saved link or preset of one group asks
    // the server the question it always asked.
    const audience = {
      ...emptyAudience(),
      groups: [{ maxRecencyDays: 180, filters: { ordersMin: 2, brands: ['Anua'] } }],
    }
    expect(audienceToParams(audience)).toBe(
      'ltv_basis=margin&holdout_pct=10&grouping=single&max_recency_days=180'
      + '&orders_min=2&brand=Anua',
    )
  })

  it('sends the first group flat and the rest as one JSON parameter', () => {
    const p = new URLSearchParams(audienceToParams(OWNER))
    expect(p.get('max_recency_days')).toBe('365')
    expect(p.get('period_orders_min')).toBe('2')
    expect(p.get('period_from')).toBe('2026-01-01')
    expect(JSON.parse(p.get('or_groups')!)).toEqual([{ max_recency_days: 92 }])
  })

  it('types the JSON as the server reads it, strictly', () => {
    // The server refuses "2" where it expects 2, and a comma string where it
    // expects a list — so the page must not send either.
    const p = new URLSearchParams(audienceToParams({
      ...emptyAudience(),
      groups: [
        emptyGroup(),
        {
          maxRecencyDays: 400,
          filters: {
            ordersMin: 2, ltvMin: 1500.5, periodDays: 60, periodOrdersMin: 1,
            cities: ['Київ', 'Lviv'], categoryIds: [10, 11], genders: ['f'],
            promocodeUsed: ' KS-AUG ', firstOrderFrom: '2025-01-01',
          },
        },
      ],
    }))
    expect(JSON.parse(p.get('or_groups')!)).toEqual([{
      max_recency_days: 400, orders_min: 2, ltv_min: 1500.5, period_days: 60,
      period_orders_min: 1, city: ['Київ', 'Lviv'], category_id: [10, 11],
      gender: ['f'], promocode_used: 'KS-AUG', first_order_from: '2025-01-01',
    }])
  })

  it('keeps everything common to the campaign out of the groups', () => {
    const p = new URLSearchParams(audienceToParams({
      ...OWNER, tiers: ['VIP'], grouping: 'rfm', holdoutPct: 20,
    }, { campaign: 'owner-oct' }))
    expect(p.get('tier')).toBe('VIP')
    expect(p.get('grouping')).toBe('rfm')
    expect(p.get('campaign')).toBe('owner-oct')
    for (const g of JSON.parse(p.get('or_groups')!)) {
      expect(Object.keys(g)).not.toContain('tier')
      expect(Object.keys(g)).not.toContain('holdout_pct')
    }
  })
})

describe('groups in a preset and a draft', () => {
  it('round-trips through what the page saves', () => {
    const saved = JSON.parse(JSON.stringify(audienceToPreset(OWNER)))
    expect(audienceFromPreset(saved)).toEqual(OWNER)
  })

  it('also writes the first group where an older page reads it', () => {
    // An older page opening this preset sees its first group, not an empty
    // audience of everyone.
    const saved = audienceToPreset(OWNER)
    expect(saved.maxRecencyDays).toBe(365)
    expect(saved.filters).toEqual({ periodOrdersMin: 2, periodFrom: '2026-01-01' })
  })

  it('opens a preset from before groups as its one group', () => {
    const loaded = audienceFromPreset({
      grouping: 'rfm', maxRecencyDays: 270, filters: { ordersMin: 2 },
    })
    expect(loaded.groups).toEqual([{ maxRecencyDays: 270, filters: { ordersMin: 2 } }])
  })

  it('opens the built-in audiences, which predate groups', () => {
    const rfm = audienceFromPreset({
      grouping: 'rfm', ltvBasis: 'margin', maxRecencyDays: 270, coreMinOrders: 2,
      reactivationMaxRecency: 120, salesType: 'retail', holdoutPct: 10, filters: {},
    })
    expect(rfm.groups).toEqual([emptyGroup()])
    expect(rfm.grouping).toBe('rfm')
  })

  it('reads no more groups than the server accepts', () => {
    const loaded = audienceFromPreset({
      groups: Array.from({ length: MAX_AUDIENCE_GROUPS + 3 }, emptyGroup),
    })
    expect(loaded.groups).toHaveLength(MAX_AUDIENCE_GROUPS)
  })

  it('keeps the date when a stored period says it both ways', () => {
    const loaded = audienceFromPreset({
      groups: [{ maxRecencyDays: 270,
                 filters: { periodFrom: '2026-01-01', periodDays: 30, periodOrdersMin: 2 } }],
    })
    expect(loaded.groups[0].filters).toEqual({ periodFrom: '2026-01-01', periodOrdersMin: 2 })
  })
})
