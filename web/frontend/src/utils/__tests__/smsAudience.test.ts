import { describe, it, expect } from 'vitest'
import {
  audienceFromPreset,
  audienceToParams,
  countFilters,
  describeAudience,
  emptyAudience,
  hasFilters,
  invertedRanges,
} from '../smsAudience'

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

    // An unrecognised split falls back to the default one.
    expect(loaded.grouping).toBe('single')
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

describe('describeAudience', () => {
  const t = (key: string, opts?: Record<string, unknown>) =>
    opts ? `${key}(${JSON.stringify(opts)})` : key

  it('says outright when an audience has no filters', () => {
    // Two of the audiences that ship with the page are exactly this, and
    // picking one changes nothing on screen.
    expect(describeAudience(emptyAudience(), t)).toContain('sms.summaryNoFilters')
  })

  it('counts the filters that are set', () => {
    const described = describeAudience({
      ...emptyAudience(),
      filters: { brands: ['Anua'], recencyMin: 90, firstOrderFrom: '2026-01-01' },
    }, t)

    expect(described).toContain('sms.summaryFilters({"count":3})')
  })

  it('states the split, the basis, the holdout and the window', () => {
    const described = describeAudience({
      ...emptyAudience(), grouping: 'single', holdoutPct: 30, maxRecencyDays: 540,
    }, t)

    expect(described).toContain('sms.grouping.single')
    expect(described).toContain('sms.basisMargin')
    expect(described).toContain('sms.summaryHoldout({"pct":30})')
    expect(described).toContain('sms.summaryWindow({"days":540})')
  })
})

describe('invertedRanges', () => {
  it('names the pair being typed backwards', () => {
    // "from 90" typed, "to 2" half-typed on the way to 237 — the state every
    // range passes through, and it used to earn a 400 from the server.
    expect(invertedRanges({ recencyMin: 90, recencyMax: 2 })).toEqual(['recency'])
    expect(invertedRanges({ firstOrderFrom: '2026-06-01', firstOrderTo: '2026-01-01' }))
      .toEqual(['firstOrder'])
  })

  it('says nothing about a range with one edge, or a sane one', () => {
    expect(invertedRanges({ recencyMin: 90 })).toEqual([])
    expect(invertedRanges({ recencyMin: 90, recencyMax: 237 })).toEqual([])
    expect(invertedRanges({})).toEqual([])
  })
})
