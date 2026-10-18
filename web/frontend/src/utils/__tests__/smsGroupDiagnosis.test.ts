import { describe, it, expect } from 'vitest'
import {
  diagnoseGroup, diagnosisProbes, emptyKind, LEVEL, probeCount,
} from '../smsGroupDiagnosis'
import {
  emptyDefinition, type AudienceDefinition, type Condition, type ConditionGroup,
} from '../smsConditions'
import type { SmsSegmentsResponse } from '../../types/api'

const TODAY = '2026-10-18'

const group = (...conditions: Condition[]): ConditionGroup => ({ id: 'g', conditions })
const def = (...groups: ConditionGroup[]): AudienceDefinition => ({ ...emptyDefinition(), groups })

const many: Condition = {
  id: 'many', type: 'orders', op: 'atLeast', count: 92, countMax: null, period: { kind: 'all' },
}
const recent: Condition = { id: 'recent', type: 'lastOrder', when: { kind: 'within', days: 90 } }
const kyiv: Condition = { id: 'kyiv', type: 'city', cities: ['Київ'] }
const men: Condition = { id: 'men', type: 'gender', genders: ['m'], confidence: null }
const unfinishedBrand: Condition = {
  id: 'brand', type: 'bought', brands: [], categoryIds: [], promocode: '', withinDays: null,
}

describe('which groups are asked about', () => {
  it('a group whose own conditions match nobody, and one nobody can be messaged in', () => {
    expect(emptyKind({ group: 1, matched: 0, recipients: 0, onlyHere: 0 })).toBe('nobody')
    expect(emptyKind({ group: 1, matched: 5, recipients: 0, onlyHere: 0 })).toBe('unreachable')
    expect(emptyKind({ group: 1, matched: 5, recipients: 3, onlyHere: 0 })).toBeNull()
    expect(emptyKind(undefined)).toBeNull()
  })
})

describe('the previews asked', () => {
  it('asks nothing for a group of one condition: that condition is the answer', () => {
    expect(diagnosisProbes(def(group(many)), 0, 'nobody', TODAY)).toEqual([])
  })

  it('asks the group alone once without each condition, and nothing else', () => {
    const d = def(group(recent), group(many, recent, kyiv))
    const probes = diagnosisProbes(d, 1, 'nobody', TODAY)

    expect(probes.map((p) => p.without)).toEqual(['many', 'recent', 'kyiv'])
    const withoutMany = new URLSearchParams(probes[0].params)
    expect(withoutMany.has('orders_min')).toBe(false)
    expect(withoutMany.get('max_recency_days')).toBe('90')
    expect(withoutMany.get('city')).toBe('Київ')
    // The group alone: the other group plays no part in what this one matches.
    expect(withoutMany.has('or_groups')).toBe(false)
    // No campaign name: a count is the same whatever the campaign is called,
    // and one cache entry serves every name typed.
    expect(withoutMany.has('campaign')).toBe(false)
  })

  it('leaves an unfinished condition out, as the query does', () => {
    const probes = diagnosisProbes(def(group(many, unfinishedBrand, recent)), 0, 'nobody', TODAY)
    expect(probes.map((p) => p.without)).toEqual(['many', 'recent'])
  })

  it('asks about the campaign level too when nobody can be messaged', () => {
    const d = { ...def(group(kyiv, men)), tiers: ['VIP' as const] }
    const probes = diagnosisProbes(d, 0, 'unreachable', TODAY)

    expect(probes.map((p) => p.without)).toEqual(['kyiv', 'men', LEVEL])
    expect(new URLSearchParams(probes[2].params).has('tier')).toBe(false)
    expect(new URLSearchParams(probes[0].params).get('tier')).toBe('VIP')
  })
})

describe('the answer', () => {
  it("is the group's only condition, without a single preview", () => {
    expect(diagnoseGroup(def(group(many)), 0, 'nobody', {}))
      .toEqual({ kind: 'alone', conditionId: 'many' })
  })

  it('waits for every preview, and says nothing if one failed', () => {
    const d = def(group(many, recent))
    expect(diagnoseGroup(d, 0, 'nobody', { many: 3000 })).toBe('pending')
    expect(diagnoseGroup(d, 0, 'nobody', { many: 3000, recent: null })).toBeNull()
  })

  it('of two, names the one that matches nobody even on its own', () => {
    // Without "92+ orders" 3 000 match; without "recent", 92+ alone matches 0.
    expect(diagnoseGroup(def(group(many, recent)), 0, 'nobody', { many: 3000, recent: 0 }))
      .toEqual({ kind: 'alone', conditionId: 'many' })
  })

  it('of three, names the one whose removal brings people back', () => {
    expect(diagnoseGroup(def(group(many, recent, kyiv)), 0, 'nobody',
      { many: 812, recent: 0, kyiv: 0 }))
      .toEqual({ kind: 'cuts', conditionId: 'many', without: 812 })
  })

  it('names a pair that excludes each other', () => {
    expect(diagnoseGroup(def(group(many, recent, kyiv)), 0, 'nobody',
      { many: 0, recent: 40, kyiv: 7 }))
      .toEqual({ kind: 'clash', conditionIds: ['recent', 'kyiv'] })
  })

  it('says when neither of two matches anybody, and when no one condition is to blame', () => {
    expect(diagnoseGroup(def(group(many, kyiv)), 0, 'nobody', { many: 0, kyiv: 0 }))
      .toEqual({ kind: 'neither', conditionIds: ['many', 'kyiv'] })
    expect(diagnoseGroup(def(group(many, recent, kyiv)), 0, 'nobody',
      { many: 0, recent: 0, kyiv: 0 }))
      .toEqual({ kind: 'together' })
  })

  it('blames the campaign level first when nobody can be messaged', () => {
    const d = { ...def(group(kyiv, men)), tiers: ['VIP' as const] }
    expect(diagnoseGroup(d, 0, 'unreachable', { kyiv: 0, men: 4, [LEVEL]: 37 }))
      .toEqual({ kind: 'level', without: 37 })
  })

  it('else names the condition whose removal reaches the most people', () => {
    expect(diagnoseGroup(def(group(kyiv, men)), 0, 'unreachable', { kyiv: 120, men: 9 }))
      .toEqual({ kind: 'reach', conditionId: 'kyiv', without: 120 })
    expect(diagnoseGroup(def(group(kyiv, men)), 0, 'unreachable', { kyiv: 0, men: 0 }))
      .toBeNull()
  })
})

describe('what a preview counts', () => {
  const answer = (matched: number, customers: number) => ({
    groups: [{ group: 1, matched, recipients: customers, onlyHere: customers }],
    totals: { customers, target: customers, holdout: 0 },
  }) as unknown as SmsSegmentsResponse

  it("reads the group's own matches for nobody, and the people reached for unreachable", () => {
    expect(probeCount('nobody', answer(3000, 2000))).toBe(3000)
    expect(probeCount('unreachable', answer(3000, 2000))).toBe(2000)
  })
})
