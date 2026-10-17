import { afterEach, describe, it, expect } from 'vitest'
import i18n from '../../lib/i18n'
import {
  audienceSentence, conditionSentence, describeFrozenCriteria, plural,
} from '../smsConditionText'
import {
  emptyDefinition, type AudienceDefinition, type Condition, type OrdersCondition,
} from '../smsConditions'

afterEach(() => { void i18n.changeLanguage('en') })

const t = (key: string, opts?: Record<string, unknown>) => i18n.t(key, opts) as string
const plain = (s: string) => s.replace(/[  ]/g, ' ')

const orders = (count: number, from: string, to: string): OrdersCondition => ({
  id: `o${count}${from}`, type: 'orders', op: 'atLeast', count, countMax: null,
  period: { kind: 'range', from, to },
})

const OWNER: AudienceDefinition = {
  ...emptyDefinition(),
  groups: [
    { id: 'a', conditions: [orders(2, '2026-01-01', '2026-10-14')] },
    { id: 'b', conditions: [orders(1, '2026-07-16', '2026-10-14')] },
  ],
}

describe('the plain-language summary', () => {
  it("says the owner's audience the way the spec wrote it", async () => {
    await i18n.changeLanguage('ru')
    expect(plain(audienceSentence(OWNER, t, { count: 4101 }))).toBe(
      'Клиенты, у которых 2+ заказа с 1 янв по 14 окт 2026, '
      + 'или 1+ заказ с 16 июл по 14 окт 2026 — 4 101 человек',
    )
  })

  it('is every buyer while any group is empty', async () => {
    await i18n.changeLanguage('uk')
    const d = { ...OWNER, groups: [...OWNER.groups, { id: 'c', conditions: [] }] }
    expect(audienceSentence(d, t)).toBe('Усі покупці')
  })

  it('joins a group with "and", and leaves out what is not filled in', () => {
    const d: AudienceDefinition = {
      ...emptyDefinition(),
      tiers: ['VIP'],
      groups: [{
        id: 'a',
        conditions: [
          { id: 'l', type: 'lastOrder', when: { kind: 'before', days: 60 } },
          { id: 'b', type: 'bought', brands: ['Anua'], categoryIds: [], promocode: '', withinDays: 180 },
          { id: 'x', type: 'city', cities: [] },
        ],
      }],
    }
    expect(audienceSentence(d, t)).toBe(
      'Customers with last order 60+ days ago and purchases of Anua in the last 180 days'
      + '; value level: VIP',
    )
  })
})

describe('a purchase and a channel in one group', () => {
  it('are one clause, because the server asks both of one order line', async () => {
    await i18n.changeLanguage('ru')
    const d: AudienceDefinition = {
      ...emptyDefinition(),
      groups: [{ id: 'a', conditions: [
        { id: 'b', type: 'bought', brands: ['Anua'], categoryIds: [], promocode: '', withinDays: 30 },
        { id: 's', type: 'source', sourceIds: [1, 4] },
      ] }],
    }
    expect(audienceSentence(d, t)).toBe(
      'Клиенты, у которых покупки Anua через Instagram или Сайт за последние 30 дней',
    )
  })
})

describe('a condition as its row', () => {
  it('puts the unit in the sentence, in the form the number takes', async () => {
    await i18n.changeLanguage('ru')
    const c = (count: number): Condition => ({ ...orders(count, '', ''), period: { kind: 'last', days: 21 } })
    expect(conditionSentence(c(1), t).blanks).toEqual(['1+ заказ', 'за последние 21 день'])
    expect(conditionSentence(c(2), t).blanks[0]).toBe('2+ заказа')
    expect(conditionSentence(c(5), t).blanks[0]).toBe('5+ заказов')
    expect(conditionSentence(
      { ...orders(3, '', ''), op: 'atMost', period: { kind: 'all' } }, t,
    ).blanks).toEqual(['не больше 3 заказов', 'за всё время'])
    expect(conditionSentence(c(2), t).label).toBe('Сделал(а)')
  })

  it('picks plural forms by the rules of the language', async () => {
    await i18n.changeLanguage('uk')
    expect(plural(t, 'word.days', 1)).toBe('день')
    expect(plural(t, 'word.days', 3)).toBe('дні')
    expect(plural(t, 'word.days', 11)).toBe('днів')
    await i18n.changeLanguage('en')
    expect(plural(t, 'word.days', 1)).toBe('day')
    expect(plural(t, 'word.days', 2)).toBe('days')
  })
})

describe('a frozen campaign in the same words', () => {
  it('says how it was measured, then who, group by group', async () => {
    await i18n.changeLanguage('ru')
    const lines = describeFrozenCriteria({
      grouping: 'single', ltvBasis: 'margin', holdoutPct: 10,
      groups: [
        { maxRecencyDays: 3650, filters: { period_orders_min: 2, period_from: '2026-01-01', period_to: '2026-10-14' } },
        { maxRecencyDays: 92, filters: {} },
      ],
    }, t).map(plain)
    expect(lines[0]).toBe('Одним плечом · По марже · контроль 10%')
    expect(lines[1]).toBe('Группа 1: 2+ заказа с 1 янв по 14 окт 2026')
    expect(lines[2]).toBe('Группа 2: последний заказ в последние 92 дня')
  })

  it('reads a one-group campaign frozen before groups existed', () => {
    const lines = describeFrozenCriteria({
      grouping: 'rfm', maxRecencyDays: 270, filters: { period_days: 92 },
    }, t)
    expect(lines).toEqual([
      'By value level',
      'Customers with last order in the last 270 days and 1+ order in the last 92 days',
    ])
  })
})
