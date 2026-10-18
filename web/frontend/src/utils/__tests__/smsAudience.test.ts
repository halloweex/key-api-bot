import { describe, it, expect } from 'vitest'
import {
  ALL_TIME_DAYS,
  audienceFromPreset,
  audienceToParams,
  emptyAudience,
  MAX_WINDOW_DAYS,
} from '../smsAudience'
import type { SmsAudienceCriteria, SmsAudienceFilters } from '../../types/api'

/** An audience of one group — what every audience was before groups. */
function oneGroup(
  filters: SmsAudienceFilters, rest: Partial<SmsAudienceCriteria> = {}, days = 270,
): SmsAudienceCriteria {
  return { ...emptyAudience(), ...rest, groups: [{ maxRecencyDays: days, filters }] }
}

describe('audienceToParams', () => {
  it('sends one arm and no rules when nothing is chosen', () => {
    const p = new URLSearchParams(audienceToParams(emptyAudience()))

    // One arm by default: the tier cascade drops whoever matches none of its
    // three conditions, and a default must not remove people unasked.
    expect(p.get('grouping')).toBe('single')
    expect(p.get('ltv_basis')).toBe('margin')
    expect(p.get('holdout_pct')).toBe('10')
    // An unset filter is not a parameter: the API's defaults are the cohort
    // every campaign before filters existed was built from.
    expect(p.has('recency_min')).toBe(false)
    expect(p.has('brand')).toBe(false)
    expect(p.has('tier')).toBe(false)
  })

  it('carries every filter family under its API name', () => {
    const p = new URLSearchParams(audienceToParams(oneGroup({
      recencyMin: 30, recencyMax: 90,
      ordersMin: 2, ltvMin: 1000, aovMax: 5000,
      firstOrderFrom: '2026-01-01',
      cities: ['Kyiv', 'Lviv'],
      brands: ['Medi-Peel', 'Anua'],
      categoryIds: [10, 11],
      sourceIds: [1],
      promocodeUsed: 'KS-AUG',
      boughtWithinDays: 90,
    }, { grouping: 'single' })))

    expect(p.get('recency_min')).toBe('30')
    expect(p.get('recency_max')).toBe('90')
    expect(p.get('orders_min')).toBe('2')
    expect(p.get('ltv_min')).toBe('1000')
    expect(p.get('aov_max')).toBe('5000')
    expect(p.get('first_order_from')).toBe('2026-01-01')
    expect(p.get('city')).toBe('Kyiv,Lviv')
    expect(p.get('brand')).toBe('Medi-Peel,Anua')
    expect(p.get('category_id')).toBe('10,11')
    expect(p.get('source_id')).toBe('1')
    expect(p.get('promocode_used')).toBe('KS-AUG')
    expect(p.get('bought_within_days')).toBe('90')
  })

  it('keeps a zero, which is a real bound', () => {
    const p = new URLSearchParams(audienceToParams(oneGroup({ recencyMin: 0, ltvMin: 0 })))

    expect(p.get('recency_min')).toBe('0')
    expect(p.get('ltv_min')).toBe('0')
  })

  it('sends picked tiers, and none when all are wanted', () => {
    const base = { ...emptyAudience(), grouping: 'rfm' as const }
    expect(new URLSearchParams(audienceToParams(base)).has('tier')).toBe(false)
    expect(
      new URLSearchParams(audienceToParams({ ...base, tiers: ['CORE', 'REACTIVATION'] }))
        .get('tier'),
    ).toBe('CORE,REACTIVATION')
  })

  it('carries the level cut-offs under either way of measuring', () => {
    // The levels do two jobs: they filter ("send to VIP only") and they can
    // split the measurement. The cut-offs define them either way.
    const rules = { vipLtv: 8000, coreLtv: 3000, coreMinOrders: 3,
                    reactivationMaxRecency: 90 }

    for (const grouping of ['rfm', 'single'] as const) {
      const p = new URLSearchParams(audienceToParams({
        ...emptyAudience(), grouping, tierRules: rules,
      }))
      expect(p.get('vip_ltv')).toBe('8000')
      expect(p.get('core_ltv')).toBe('3000')
      expect(p.get('core_min_orders')).toBe('3')
      expect(p.get('reactivation_max_recency')).toBe('90')
    }
  })

  it('keeps the level filter under a single-arm measurement', () => {
    // "Send to VIP only, measured as one group" is a legitimate campaign, and
    // the commonest one: splitting costs sensitivity that these audiences do
    // not have to spare.
    const p = new URLSearchParams(audienceToParams({
      ...emptyAudience(), grouping: 'single', tiers: ['VIP'],
    }))

    expect(p.get('tier')).toBe('VIP')
    expect(p.get('grouping')).toBe('single')
  })

  it('appends the campaign name and promocode when given', () => {
    const p = new URLSearchParams(audienceToParams(emptyAudience(), {
      campaign: 'sep-brand', promocode: 'KS-SEP', empty: '',
    }))

    expect(p.get('campaign')).toBe('sep-brand')
    expect(p.get('promocode')).toBe('KS-SEP')
    expect(p.has('empty')).toBe(false)
  })
})

describe('audienceFromPreset', () => {
  it('round-trips what the form produced', () => {
    const audience = oneGroup(
      { brands: ['Anua'], categoryIds: [10], recencyMax: 90 },
      { grouping: 'single', ltvBasis: 'revenue', holdoutPct: 30, tiers: [] },
    )

    expect(audienceFromPreset(JSON.parse(JSON.stringify(audience)))).toEqual(audience)
  })

  it('opens a preset written by an older page instead of throwing', () => {
    // Unknown fields are dropped, missing ones fall back — a preset that
    // cannot be opened is worse than one that opens with a field missing.
    const loaded = audienceFromPreset({
      grouping: 'cohorts',
      holdoutPct: 'lots',
      tiers: ['VIP', 'GOLD'],
      filters: { brands: ['Anua'], unknownThing: 5, recencyMin: 'soon' },
      somethingElse: true,
    })

    // An unrecognised split falls back to the default one.
    expect(loaded.grouping).toBe('single')
    expect(loaded.holdoutPct).toBe(10)
    expect(loaded.tiers).toEqual(['VIP'])
    expect(loaded.groups).toEqual([{ maxRecencyDays: 270, filters: { brands: ['Anua'] } }])
  })

  it('treats a non-object as no preset at all', () => {
    expect(audienceFromPreset(null)).toEqual(emptyAudience())
    expect(audienceFromPreset('rfm')).toEqual(emptyAudience())
  })
})

describe('the base window', () => {
  it('always travels, because the server applies it whether or not it is sent', () => {
    const p = new URLSearchParams(audienceToParams(emptyAudience()))
    expect(p.get('max_recency_days')).toBe('270')
  })

  it('carries a widened window', () => {
    const p = new URLSearchParams(audienceToParams(oneGroup({}, {}, 730)))
    expect(p.get('max_recency_days')).toBe('730')
  })

  it('reaches as far as the server allows, which is all time', () => {
    // The page no longer shows a window: a group with no "last order"
    // condition sends the ceiling, and the ceiling covers the whole history.
    expect(ALL_TIME_DAYS).toBe(MAX_WINDOW_DAYS)
    expect(MAX_WINDOW_DAYS).toBe(3650)
    const p = new URLSearchParams(audienceToParams(oneGroup({}, {}, ALL_TIME_DAYS)))
    expect(p.get('max_recency_days')).toBe('3650')
  })

  it('comes back from a preset, and falls back when one predates it', () => {
    expect(audienceFromPreset({ maxRecencyDays: 540 }).groups[0].maxRecencyDays).toBe(540)
    expect(audienceFromPreset({ grouping: 'single' }).groups[0].maxRecencyDays).toBe(270)
  })
})

describe('orders in a period', () => {
  it('sends the end of the period and an upper count under their API names', () => {
    const p = new URLSearchParams(audienceToParams(oneGroup({
      periodOrdersMin: 2, periodOrdersMax: 3, periodFrom: '2026-01-01', periodTo: '2026-10-14',
    })))
    expect(p.get('period_orders_min')).toBe('2')
    expect(p.get('period_orders_max')).toBe('3')
    expect(p.get('period_from')).toBe('2026-01-01')
    expect(p.get('period_to')).toBe('2026-10-14')
  })

  it('keeps an upper count of zero — "none in the period" is a real condition', () => {
    const p = new URLSearchParams(audienceToParams(oneGroup({
      periodOrdersMax: 0, periodDays: 90,
    })))
    expect(p.get('period_orders_max')).toBe('0')
    expect(p.has('period_orders_min')).toBe(false)
  })

  it('reads both back from a preset, and keeps the dates over the days', () => {
    const restored = audienceFromPreset({
      filters: { periodOrdersMax: 0, periodTo: '2026-10-14', periodDays: 30 },
    })
    expect(restored.groups[0].filters).toEqual({ periodOrdersMax: 0, periodTo: '2026-10-14' })
  })
})

describe('the gender filter', () => {
  it('travels as flat query parameters, like every other filter', () => {
    // Flat rather than a JSON body because the CSV download is a link the
    // browser follows, and the session gate reads the query string.
    const p = new URLSearchParams(audienceToParams(
      oneGroup({ genders: ['m'], genderMinConfidence: 'high' }),
    ))
    expect(p.get('gender')).toBe('m')
    expect(p.get('gender_min_confidence')).toBe('high')
  })

  it('sends both values comma-separated, like every other list filter', () => {
    // `city` and `brand` travel the same way and the server splits all three
    // with the same `_text_list`. A repeated parameter would need a second
    // parsing rule for no gain.
    const p = new URLSearchParams(audienceToParams(oneGroup({ genders: ['f', 'm'] })))
    expect(p.get('gender')).toBe('f,m')
  })

  it('is not a parameter when it is not set', () => {
    // An empty filter set has to select exactly what it selected before this
    // existed, or every past campaign becomes unreproducible.
    const p = new URLSearchParams(audienceToParams(emptyAudience()))
    expect(p.has('gender')).toBe(false)
    expect(p.has('gender_min_confidence')).toBe(false)
  })

  it('survives a round trip through a saved preset', () => {
    const restored = audienceFromPreset({
      filters: { genders: ['m'], genderMinConfidence: 'certain' },
    })
    expect(restored.groups[0].filters.genders).toEqual(['m'])
    expect(restored.groups[0].filters.genderMinConfidence).toBe('certain')
  })

  it('drops a value the API would refuse', () => {
    // A preset is form state somebody saved. A stored 'x' would reach the API
    // as a 400 on a screen the person only opened to look at.
    const restored = audienceFromPreset({
      filters: { genders: ['f', 'x', ''], genderMinConfidence: 'nonsense' },
    })
    expect(restored.groups[0].filters.genders).toEqual(['f'])
    expect(restored.groups[0].filters.genderMinConfidence).toBeUndefined()
  })
})
