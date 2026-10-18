import { beforeEach, describe, it, expect } from 'vitest'
import { clearDraft, emptyDraft, loadDraft, saveDraft } from '../smsDraft'

beforeEach(() => sessionStorage.clear())

describe('the draft, v2', () => {
  it('reads a v1 draft — the server shape — into conditions', () => {
    sessionStorage.setItem('sms.campaignDraft.v1', JSON.stringify({
      campaign: 'oct-old', text: 'Знижка', followUp: false,
      audience: {
        grouping: 'single', ltvBasis: 'margin', holdoutPct: 20, tiers: [], tierRules: {},
        groups: [{ maxRecencyDays: 365, filters: { periodOrdersMin: 2, periodFrom: '2026-01-01' } }],
      },
    }))
    const d = loadDraft()
    expect(d.campaign).toBe('oct-old')
    expect(d.audience.holdoutPct).toBe(20)
    expect(d.audience.groups[0].conditions).toMatchObject([
      { type: 'lastOrder', when: { kind: 'within', days: 365 } },
      { type: 'orders', op: 'atLeast', count: 2,
        period: { kind: 'range', from: '2026-01-01', to: '' } },
    ])
  })

  it('writes v2, retires v1, and reads its own conditions back unchanged', () => {
    sessionStorage.setItem('sms.campaignDraft.v1', '{}')
    const draft = emptyDraft()
    draft.audience.groups[0].conditions.push({
      id: 'x', type: 'orders', op: 'atLeast', count: 2, countMax: null,
      period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' },
    })
    saveDraft(draft)
    expect(sessionStorage.getItem('sms.campaignDraft.v1')).toBeNull()
    expect(loadDraft().audience.groups[0].conditions).toMatchObject([
      { type: 'orders', period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' } },
    ])
  })

  it('costs a draft, never the page, when the store holds garbage', () => {
    sessionStorage.setItem('sms.campaignDraft.v2', '{not json')
    expect(loadDraft().audience.groups).toHaveLength(1)
    clearDraft()
    expect(sessionStorage.getItem('sms.campaignDraft.v2')).toBeNull()
  })
})
