import { describe, it, expect } from 'vitest'
import {
  allowedPeriodKinds, applyTemplate, canAdd, conditionProblem, daysSince,
  definitionFromCriteria, definitionFromFrozen, definitionFromPreset, definitionToCriteria,
  definitionToPreset, emptyDefinition, emptyExtraGroups, groupFromCriteria, groupToCriteria,
  hasAbsoluteDates, tierRuleProblems,
  moveCountIntoPeriod, newCondition, shiftDates, suspiciousCount, TEMPLATE_KEYS, unfinished,
  type AudienceDefinition, type Condition, type ConditionGroup, type OrdersCondition,
} from '../smsConditions'
import { ALL_TIME_DAYS, audienceFromPreset, audienceToParams } from '../smsAudience'

// Every date below is resolved against this day, so nothing here moves with
// the calendar the suite happens to run on.
const TODAY = '2026-10-17'

const group = (...conditions: Condition[]): ConditionGroup => ({ id: 'g', conditions })
const def = (...groups: ConditionGroup[]): AudienceDefinition =>
  ({ ...emptyDefinition(), groups })
const orders = (over: Partial<OrdersCondition>): OrdersCondition => ({
  id: 'o', type: 'orders', op: 'atLeast', count: 1, countMax: null, period: { kind: 'all' },
  ...over,
})

function params(d: AudienceDefinition): URLSearchParams {
  return new URLSearchParams(audienceToParams(definitionToCriteria(d, TODAY)))
}

// The owner's campaign of 2026-10-17, said the way the new step says it.
const OWNER = def(
  group(orders({ count: 2, period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' } })),
  group(orders({ count: 1, period: { kind: 'range', from: '2026-07-16', to: '2026-10-14' } })),
)

describe('conditions to the server', () => {
  it("sends the owner's audience as two periods with both ends, and no hidden window", () => {
    const p = params(OWNER)
    // All time: a period starting in January is no longer cut by a 270-day
    // window nobody set.
    expect(p.get('max_recency_days')).toBe(String(ALL_TIME_DAYS))
    expect(p.get('period_orders_min')).toBe('2')
    expect(p.get('period_from')).toBe('2026-01-01')
    expect(p.get('period_to')).toBe('2026-10-14')
    expect(JSON.parse(p.get('or_groups')!)).toEqual([{
      max_recency_days: ALL_TIME_DAYS, period_orders_min: 1,
      period_from: '2026-07-16', period_to: '2026-10-14',
    }])
  })

  it('says one count with a period instead of two kinds of count', () => {
    const lifetime = params(def(group(orders({ op: 'exactly', count: 1 }))))
    expect(lifetime.get('orders_min')).toBe('1')
    expect(lifetime.get('orders_max')).toBe('1')
    expect(lifetime.has('period_orders_min')).toBe(false)

    const inPeriod = params(def(group(orders({ op: 'atMost', count: 3, period: { kind: 'last', days: 92 } }))))
    expect(inPeriod.get('period_orders_max')).toBe('3')
    expect(inPeriod.get('period_days')).toBe('92')
    expect(inPeriod.has('period_orders_min')).toBe(false)
    expect(inPeriod.has('orders_max')).toBe(false)
  })

  it('says "none in the period" as an upper count of zero', () => {
    const p = params(def(group(orders({ op: 'none', count: null, period: { kind: 'last', days: 90 } }))))
    expect(p.get('period_orders_max')).toBe('0')
    expect(p.has('period_orders_min')).toBe(false)
  })

  it('derives the window from the last order, which is the shape the audience always had', () => {
    const within = params(def(group({ id: 'l', type: 'lastOrder', when: { kind: 'within', days: 270 } })))
    expect(within.get('max_recency_days')).toBe('270')
    expect(within.has('recency_max')).toBe(false)

    // The server's window starts at 30; narrower is the floor plus the bound.
    const narrow = params(def(group({ id: 'l', type: 'lastOrder', when: { kind: 'within', days: 7 } })))
    expect(narrow.get('max_recency_days')).toBe('30')
    expect(narrow.get('recency_max')).toBe('7')

    const quiet = params(def(group({ id: 'l', type: 'lastOrder', when: { kind: 'ago', min: 90, max: 180 } })))
    expect(quiet.get('max_recency_days')).toBe('180')
    expect(quiet.get('recency_min')).toBe('90')
  })

  it('resolves last-order dates against today, the later date being the fewer days', () => {
    const p = params(def(group({
      id: 'l', type: 'lastOrder', when: { kind: 'dates', from: '2026-07-19', to: '2026-09-17' },
    })))
    expect(p.get('recency_min')).toBe('30')
    expect(p.get('max_recency_days')).toBe('90')
  })

  it('resolves a relative first order into dates the server reads', () => {
    expect(params(def(group({ id: 'f', type: 'firstOrder', when: { kind: 'within', days: 30 } })))
      .get('first_order_from')).toBe('2026-09-17')
    const p = params(def(group({ id: 'f', type: 'firstOrder', when: { kind: 'ago', min: 30, max: 90 } })))
    expect(p.get('first_order_from')).toBe('2026-07-19')
    expect(p.get('first_order_to')).toBe('2026-09-17')
  })

  it('leaves an unfinished condition out, and says so', () => {
    const d = def(group(
      orders({ count: 2 }),
      { id: 'b', type: 'bought', brands: [], categoryIds: [], promocode: '', withinDays: null },
    ))
    expect(params(d).get('orders_min')).toBe('2')
    expect([...params(d).keys()]).not.toContain('brand')
    expect(unfinished(d)).toEqual([{ group: 1, id: 'b', problem: 'incomplete' }])
  })

  it('keeps a reactivation cut-off inside the window it is derived', () => {
    const d = {
      ...def(group({ id: 'l', type: 'lastOrder', when: { kind: 'within', days: 60 } })),
      tierRules: { reactivationMaxRecency: 120 },
    }
    expect(params(d).get('reactivation_max_recency')).toBe('60')
  })
})

describe('the way back: from what the server shape held', () => {
  it('maps the built-in presets onto conditions that ask exactly what they asked', () => {
    // "RFM tiers" must keep meaning what past campaigns meant by it.
    const rfm = { grouping: 'rfm', ltvBasis: 'margin', maxRecencyDays: 270, coreMinOrders: 2,
                  reactivationMaxRecency: 120, salesType: 'retail', holdoutPct: 10, filters: {} }
    const loaded = definitionFromPreset(rfm)
    expect(loaded.groups[0].conditions).toMatchObject([
      { type: 'lastOrder', when: { kind: 'within', days: 270 } },
    ])
    expect(audienceToParams(definitionToCriteria(loaded, TODAY)))
      .toBe(audienceToParams(audienceFromPreset(rfm)))

    const everyone = { grouping: 'single', ltvBasis: 'margin', maxRecencyDays: 270,
                       salesType: 'retail', holdoutPct: 20, filters: {} }
    expect(audienceToParams(definitionToCriteria(definitionFromPreset(everyone), TODAY)))
      .toBe(audienceToParams(audienceFromPreset(everyone)))
  })

  it('maps every old filter onto its condition', () => {
    const g = groupFromCriteria({
      maxRecencyDays: 365,
      filters: {
        recencyMin: 30, ordersMin: 2, ordersMax: 5, ltvMin: 1000, aovMax: 4000,
        firstOrderFrom: '2025-01-01', brands: ['Anua'], categoryIds: [10],
        promocodeUsed: 'KS', boughtWithinDays: 90, sourceIds: [1], cities: ['Київ'],
        genders: ['f'], genderMinConfidence: 'high',
        periodOrdersMin: 2, periodFrom: '2026-01-01',
      },
    })
    expect(g.conditions.map((c) => c.type)).toEqual([
      'lastOrder', 'orders', 'orders', 'firstOrder', 'spent', 'aov', 'bought', 'source',
      'city', 'gender',
    ])
    expect(g.conditions[0]).toMatchObject({ when: { kind: 'ago', min: 30, max: 365 } })
    expect(g.conditions[1]).toMatchObject({ op: 'between', count: 2, countMax: 5, period: { kind: 'all' } })
    expect(g.conditions[2]).toMatchObject({
      op: 'atLeast', count: 2, period: { kind: 'range', from: '2026-01-01', to: '' },
    })
  })

  it('round-trips every legacy group through conditions to the same parameters', () => {
    const legacy = [
      { maxRecencyDays: 270, filters: {} },
      { maxRecencyDays: 180, filters: { ordersMin: 2, brands: ['Anua'] } },
      { maxRecencyDays: 365, filters: { periodOrdersMin: 2, periodFrom: '2026-01-01' } },
      { maxRecencyDays: 92, filters: { periodDays: 30 } },
      { maxRecencyDays: 730, filters: { recencyMin: 90, recencyMax: 400, ltvMin: 0 } },
      { maxRecencyDays: 270, filters: { genders: ['m' as const], cities: ['Lviv'] } },
    ]
    for (const g of legacy) {
      const back = groupToCriteria(groupFromCriteria(g), TODAY)
      // The same people: the window ends where the stricter of window and
      // recency_max did, and every other filter is carried as it was.
      expect(Math.min(back.maxRecencyDays, back.filters.recencyMax ?? Infinity))
        .toBe(Math.min(g.maxRecencyDays, g.filters.recencyMax ?? Infinity))
      const { recencyMax: _a, ...rest } = back.filters
      const { recencyMax: _b, ...expected } = g.filters as Record<string, unknown>
      void _a; void _b
      if ('periodOrdersMin' in expected || 'periodDays' in expected || 'periodFrom' in expected) {
        expected.periodOrdersMin ??= 1
      }
      expect(rest).toEqual(expected)
    }
  })

  it('opens a past campaign from the store names it was frozen in', () => {
    const loaded = definitionFromFrozen({
      grouping: 'single', ltvBasis: 'margin', holdoutPct: 10, maxRecencyDays: 365,
      vipLtv: 5500, coreLtv: 2750, coreMinOrders: 2, reactivationMaxRecency: 120,
      groups: [
        { maxRecencyDays: 3650, filters: { period_orders_min: 2, period_from: '2026-01-01', period_to: '2026-10-14' } },
        { maxRecencyDays: 92, filters: { brands: ['Anua'], category_ids: [10], promocode: 'KS' } },
      ],
    })
    expect(loaded.groups[0].conditions).toMatchObject([{
      type: 'orders', op: 'atLeast', count: 2,
      period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' },
    }])
    expect(loaded.groups[1].conditions).toMatchObject([
      { type: 'lastOrder', when: { kind: 'within', days: 92 } },
      { type: 'bought', brands: ['Anua'], categoryIds: [10], promocode: 'KS' },
    ])
    // The thresholds it recorded were the defaults; they are not decisions.
    expect(loaded.tierRules).toEqual({})
  })
})

describe('presets and drafts keep dates as dates', () => {
  it('stores the conditions beside the server shape, and reads the conditions first', () => {
    const d = def(group(
      orders({ count: 2, period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' } }),
      { id: 'l', type: 'lastOrder', when: { kind: 'within', days: 90 } },
    ))
    const stored = JSON.parse(JSON.stringify(definitionToPreset(d, TODAY)))
    // An older page reads this, and gets the same audience.
    expect(stored.maxRecencyDays).toBe(90)
    expect(stored.filters).toMatchObject({ periodFrom: '2026-01-01', periodTo: '2026-10-14' })
    // This page reads the conditions: relative stays relative.
    const back = definitionFromPreset(stored)
    expect(back.groups[0].conditions).toMatchObject([
      { type: 'orders', period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' } },
      { type: 'lastOrder', when: { kind: 'within', days: 90 } },
    ])
    expect(back.groups[0].conditions[0].id).not.toBe('o')
  })

  it('reads a preset from before conditions and before groups', () => {
    const back = definitionFromPreset({ grouping: 'single', holdoutPct: 30, filters: { brands: ['Anua'] } })
    expect(back.holdoutPct).toBe(30)
    expect(back.groups[0].conditions).toMatchObject([
      { type: 'lastOrder', when: { kind: 'within', days: 270 } },
      { type: 'bought', brands: ['Anua'] },
    ])
  })

  it('drops a stored condition it does not know rather than failing to open', () => {
    const back = definitionFromPreset({ groups: [{ conditions: [{ type: 'astrology' }, { type: 'city', cities: ['Київ'] }] }] })
    expect(back.groups[0].conditions).toMatchObject([{ type: 'city', cities: ['Київ'] }])
  })
})

describe('templates', () => {
  const expected: Record<string, Record<string, string>> = {
    all: { max_recency_days: '3650', orders_min: '1' },
    recent: { max_recency_days: '3650', period_orders_min: '1', period_days: '30' },
    new: { max_recency_days: '3650', first_order_from: '2026-09-17' },
    repeat: { max_recency_days: '3650', orders_min: '2' },
    oneAndGone: { max_recency_days: '3650', orders_min: '1', orders_max: '1', recency_min: '60' },
    churning: { max_recency_days: '180', recency_min: '90' },
    lost: { max_recency_days: '3650', recency_min: '240' },
    vip: { max_recency_days: '3650', tier: 'VIP' },
    brand: { max_recency_days: '3650' },
  }

  it.each(TEMPLATE_KEYS)('%s asks the server what its card says', (key) => {
    const { definition } = applyTemplate(key, emptyDefinition())
    const p = params(definition)
    const audience = Object.fromEntries(
      [...p.entries()].filter(([k]) => !['ltv_basis', 'holdout_pct', 'grouping'].includes(k)),
    )
    expect(audience).toEqual(expected[key])
  })

  it('opens "bought a brand" on the brand, which is what it needs', () => {
    const { definition, focus } = applyTemplate('brand', emptyDefinition())
    expect(focus).toBe(definition.groups[0].conditions[0].id)
    expect(unfinished(definition)).toHaveLength(1)
  })

  it('keeps how the campaign is measured', () => {
    const start = { ...emptyDefinition(), holdoutPct: 30, grouping: 'rfm' as const }
    const { definition } = applyTemplate('lost', start)
    expect(definition.holdoutPct).toBe(30)
    expect(definition.grouping).toBe('rfm')
  })
})

describe('guards against the slips of 2026-10-17', () => {
  it('suspects an order count that is really a number of days', () => {
    expect(suspiciousCount(orders({ count: 92 }))).toBe(true)
    expect(suspiciousCount(orders({ count: 20 }))).toBe(false)
    expect(moveCountIntoPeriod(orders({ count: 92 }))).toMatchObject({
      op: 'atLeast', count: 1, period: { kind: 'last', days: 92 },
    })
  })

  it('cannot hold a period whose start is after its end', () => {
    expect(conditionProblem(orders({ period: { kind: 'range', from: '2026-10-14', to: '2026-01-01' } })))
      .toBe('inverted')
    expect(conditionProblem(orders({ period: { kind: 'range', from: '', to: '' } }))).toBe('incomplete')
    // An open end is a real period: "since 1 January", "up to 14 October".
    expect(conditionProblem(orders({ period: { kind: 'range', from: '2026-01-01', to: '' } }))).toBeNull()
    expect(conditionProblem(orders({ period: { kind: 'range', from: '', to: '2026-10-14' } }))).toBeNull()
  })

  it('refuses "no orders ever" — every customer here has ordered', () => {
    expect(conditionProblem(orders({ op: 'none', count: null }))).toBe('incomplete')
  })

  it('holds a group to one lifetime count and one period count', () => {
    const lifetime = orders({ id: 'a' })
    expect(canAdd('orders', [lifetime])).toBe(true)
    const second = newCondition('orders', [lifetime]) as OrdersCondition
    expect(second.period.kind).toBe('last')
    expect(allowedPeriodKinds(second, [lifetime, second])).toEqual(['last', 'range'])
    expect(canAdd('orders', [lifetime, second])).toBe(false)
    expect(canAdd('city', [{ id: 'c', type: 'city', cities: [] }])).toBe(false)
  })
})

describe('a past campaign with dates', () => {
  it('knows it has dates, and moves all of them together', () => {
    expect(hasAbsoluteDates(OWNER)).toBe(true)
    expect(hasAbsoluteDates(def(group(orders({ period: { kind: 'last', days: 30 } }))))).toBe(false)
    const moved = shiftDates(OWNER, 16)
    expect(moved.groups[0].conditions[0]).toMatchObject({
      period: { kind: 'range', from: '2026-01-17', to: '2026-10-30' },
    })
  })

  it('counts the days since it was frozen in Kyiv days', () => {
    expect(daysSince('2026-10-01T09:00:00Z', TODAY)).toBe(16)
    // 23:30 UTC on the 16th is already the 17th in Kyiv.
    expect(daysSince('2026-10-16T23:30:00Z', TODAY)).toBe(0)
    expect(daysSince(null, TODAY)).toBe(0)
  })
})

describe('an empty audience', () => {
  it('is every buyer, over all time', () => {
    const p = params(emptyDefinition())
    expect(p.get('max_recency_days')).toBe('3650')
    expect(p.get('grouping')).toBe('single')
    expect(definitionFromCriteria(definitionToCriteria(emptyDefinition(), TODAY)).groups[0].conditions)
      .toEqual([])
  })
})

describe('what the review of 2026-10-17 found', () => {
  it('never sends two periods for one group', () => {
    const g = group(
      orders({ id: 'a', count: 1, period: { kind: 'range', from: '2026-07-16', to: '2026-10-14' } }),
      orders({ id: 'b', count: 1, period: { kind: 'last', days: 92 } }),
    )
    const f = groupToCriteria(g, TODAY).filters
    expect(f.periodFrom).toBe('2026-07-16')
    expect(f.periodDays).toBeUndefined()
  })

  it('caps a last-order date far in the past at what the server accepts', () => {
    const p = params(def(group({
      id: 'l', type: 'lastOrder', when: { kind: 'dates', from: '2000-01-01', to: '2001-01-01' },
    })))
    expect(p.get('recency_min')).toBe('3650')
    expect(p.get('max_recency_days')).toBe('3650')
  })

  it('reads "at least 0 orders" from an old preset as no condition, not an unfinished one', () => {
    const g = groupFromCriteria({ maxRecencyDays: 3650, filters: { ordersMin: 0 } })
    expect(g.conditions).toEqual([])
    const capped = groupFromCriteria({ maxRecencyDays: 3650, filters: { ordersMin: 0, ordersMax: 3 } })
    expect(capped.conditions).toMatchObject([{ type: 'orders', op: 'atMost', count: 3 }])
  })

  it('leaves a threshold the server would refuse out of the query, and names it', () => {
    const d = { ...emptyDefinition(), tierRules: { vipLtv: 6, coreMinOrders: 1, reactivationMaxRecency: 90 } }
    expect(tierRuleProblems(d)).toEqual(['vipLtv', 'coreMinOrders'])
    const p = params(d)
    expect(p.has('vip_ltv')).toBe(false)
    expect(p.has('core_min_orders')).toBe(false)
    expect(p.get('reactivation_max_recency')).toBe('90')
  })

  it('names an empty group among several — it would make the audience everyone', () => {
    expect(emptyExtraGroups(emptyDefinition())).toEqual([])
    expect(emptyExtraGroups(def(group(orders({})), { id: 'e', conditions: [] }))).toEqual([2])
  })

  it('reads the value level back from a campaign frozen with it', () => {
    expect(definitionFromFrozen({ grouping: 'single', tiers: ['VIP', 'gold'], groups: [] }).tiers)
      .toEqual(['VIP'])
  })
})
