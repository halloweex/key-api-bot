import { describe, it, expect } from 'vitest'
import {
  audienceFromPreset,
  audienceToParams,
  countFilters,
  emptyAudience,
  hasFilters,
} from '../smsAudience'

describe('audienceToParams', () => {
  it('sends the plain cohort when nothing is filtered', () => {
    const p = new URLSearchParams(audienceToParams(emptyAudience()))

    expect(p.get('grouping')).toBe('rfm')
    expect(p.get('ltv_basis')).toBe('margin')
    expect(p.get('holdout_pct')).toBe('10')
    // An unset filter is not a parameter: the API's defaults are the cohort
    // every campaign before filters existed was built from.
    expect(p.has('recency_min')).toBe(false)
    expect(p.has('brand')).toBe(false)
    expect(p.has('tier')).toBe(false)
  })

  it('carries every filter family under its API name', () => {
    const p = new URLSearchParams(audienceToParams({
      ...emptyAudience(),
      grouping: 'single',
      filters: {
        recencyMin: 30, recencyMax: 90,
        ordersMin: 2, ltvMin: 1000, aovMax: 5000,
        firstOrderFrom: '2026-01-01',
        cities: ['Kyiv', 'Lviv'],
        brands: ['Medi-Peel', 'Anua'],
        categoryIds: [10, 11],
        sourceIds: [1],
        promocodeUsed: 'KS-AUG',
        boughtWithinDays: 90,
      },
    }))

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
    const p = new URLSearchParams(audienceToParams({
      ...emptyAudience(), filters: { recencyMin: 0, ltvMin: 0 },
    }))

    expect(p.get('recency_min')).toBe('0')
    expect(p.get('ltv_min')).toBe('0')
  })

  it('sends picked tiers, and none when all are wanted', () => {
    const base = emptyAudience()
    expect(new URLSearchParams(audienceToParams(base)).has('tier')).toBe(false)
    expect(
      new URLSearchParams(audienceToParams({ ...base, tiers: ['CORE', 'REACTIVATION'] }))
        .get('tier'),
    ).toBe('CORE,REACTIVATION')
  })

  it('drops the tier subset under a single-group audience', () => {
    // One arm and a tier filter together would read as a narrowing that
    // silently does nothing.
    const p = new URLSearchParams(audienceToParams({
      ...emptyAudience(), grouping: 'single', tiers: ['VIP'],
    }))

    expect(p.has('tier')).toBe(false)
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

describe('counting filters', () => {
  it('ignores empty lists and blank strings', () => {
    const filters = { brands: [], promocodeUsed: '', recencyMin: 30 }
    expect(countFilters(filters)).toBe(1)
    expect(hasFilters(filters)).toBe(true)
    expect(hasFilters({ brands: [] })).toBe(false)
  })
})

describe('audienceFromPreset', () => {
  it('round-trips what the form produced', () => {
    const audience = {
      ...emptyAudience(),
      grouping: 'single' as const,
      ltvBasis: 'revenue' as const,
      holdoutPct: 30,
      tiers: [],
      filters: { brands: ['Anua'], categoryIds: [10], recencyMax: 90 },
    }

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

    expect(loaded.grouping).toBe('rfm')
    expect(loaded.holdoutPct).toBe(10)
    expect(loaded.tiers).toEqual(['VIP'])
    expect(loaded.filters).toEqual({ brands: ['Anua'] })
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
    const p = new URLSearchParams(audienceToParams({
      ...emptyAudience(), maxRecencyDays: 730,
    }))
    expect(p.get('max_recency_days')).toBe('730')
  })

  it('comes back from a preset, and falls back when one predates it', () => {
    expect(audienceFromPreset({ maxRecencyDays: 540 }).maxRecencyDays).toBe(540)
    expect(audienceFromPreset({ grouping: 'single' }).maxRecencyDays).toBe(270)
  })
})
