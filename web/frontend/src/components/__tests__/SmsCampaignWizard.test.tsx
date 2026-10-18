import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { SmsCampaignWizard } from '../SmsCampaignWizard'
import { suggestCampaignName } from '../../utils/smsCampaignName'
import type { SmsSegmentsResponse } from '../../types/api'

// src/lib/i18n.ts reads localStorage while the module is being imported, and
// jsdom here exposes the object without its methods.
vi.hoisted(() => {
  const store = new Map<string, string>()
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => void store.set(k, String(v)),
      removeItem: (k: string) => void store.delete(k),
      clear: () => store.clear(),
    },
  })
})

vi.mock('react-i18next', () => ({
  initReactI18next: { type: '3rdParty', init: () => {} },
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) =>
      opts ? `${key}(${JSON.stringify(opts)})` : key,
  }),
}))

const data: SmsSegmentsResponse = {
  campaign: 'default',
  salesType: 'retail',
  ltvBasis: 'margin',
  criteria: {
    maxRecencyDays: 270, ltvBasis: 'margin', vipLtv: 5500, coreLtv: 2750,
    coreMinOrders: 2, reactivationMaxRecency: 120, holdoutPct: 10,
  },
  funnel: [
    { stage: 'customers', remaining: 40000 },
    { stage: 'inWindow', remaining: 12000 },
    { stage: 'filtered', remaining: 9000 },
    { stage: 'tiered', remaining: 9000 },
    { stage: 'phone', remaining: 8600 },
    { stage: 'subscribed', remaining: 8500 },
    { stage: 'uniquePhone', remaining: 8400 },
  ],
  // Three arms, because the tier picker is what several of these tests exercise.
  segments: [
    { tier: 'VIP', total: 1000, target: 900, holdout: 100, totalLtv: 0,
      avgLtv: 12000, totalRevenue: 0, totalMargin: 0, marginPct: 55,
      avgOrders: 7.9, avgRecencyDays: 93 },
    { tier: 'CORE', total: 3000, target: 2700, holdout: 300, totalLtv: 0,
      avgLtv: 3900, totalRevenue: 0, totalMargin: 0, marginPct: 55,
      avgOrders: 2.3, avgRecencyDays: 122 },
    { tier: 'REACTIVATION', total: 4400, target: 3960, holdout: 440, totalLtv: 0,
      avgLtv: 1300, totalRevenue: 0, totalMargin: 0, marginPct: 55,
      avgOrders: 1, avgRecencyDays: 69 },
  ],
  totals: { customers: 8400, target: 7560, holdout: 840 },
  truncated: false,
}

const segmentParams: string[] = []

// Sent campaigns the follow-up list chooses from. Built against the real
// clock, because the wizard offers the last seven days of it.
const hoursAgo = (h: number) => new Date(Date.now() - h * 3600 * 1000).toISOString()
const listed = (
  campaign: string, sentAt: string | null, followUpOf: string | null = null,
  criteria?: Record<string, unknown>,
) => ({
  campaign, ltvBasis: 'margin' as const, salesType: 'retail', holdoutPct: 10,
  promocode: null, exportedAt: sentAt, sentAt, notes: null,
  members: 1000, target: 900, holdout: 100, recipientsSent: 880, followUpOf, criteria,
})
const campaignList = [
  listed('oct-promo', hoursAgo(3), null, {
    grouping: 'single', ltvBasis: 'margin', holdoutPct: 10,
    groups: [{ maxRecencyDays: 3650,
               filters: { period_orders_min: 2, period_from: '2026-01-01', period_to: '2026-10-14' } }],
  }),
  listed('oct-promo-noon', hoursAgo(1), 'oct-promo'),   // a follow-up itself
  listed('sep-promo', hoursAgo(24 * 10)),               // too old
  listed('nov-draft', null),                            // never sent
]

/** What the server answers for `follow_up_of`: the root's accepted targets,
 *  no control. */
const followUpData: SmsSegmentsResponse = {
  ...data,
  campaign: 'default',
  followUpOf: 'oct-promo',
  funnel: [
    { stage: 'rootTarget', remaining: 900 },
    { stage: 'accepted', remaining: 880 },
    { stage: 'subscribed', remaining: 871 },
  ],
  segments: [{ ...data.segments[0], tier: 'ALL', total: 871, target: 871, holdout: 0 }],
  totals: { customers: 871, target: 871, holdout: 0 },
}
// Calls back like the real mutation does, so the steps that only happen after
// a roster is frozen — the draft being cleared, the send button appearing —
// are actually exercised. The frozen roster is deliberately not the preview's:
// what the strip shows after freezing has to be the frozen numbers.
const createMutate = vi.fn((params: string, opts?: {
  onSuccess?: (r: unknown) => void
}) => {
  opts?.onSuccess?.({
    campaign: new URLSearchParams(params).get('campaign') ?? 'unnamed',
    frozen: { campaign: 'x', frozen: true },
    segments: [],
    totals: { customers: 1000, target: 901, holdout: 99 },
    funnel: [],
  })
})
const savePresetMutate = vi.fn()

/** What the fake server answers for one preview query. */
function answer(params: string): SmsSegmentsResponse {
    if (new URLSearchParams(params).has('follow_up_of')) return followUpData
    // The server applies the level filter and the split, so the fake does too
    // — a stub that ignores them would let the page display numbers the API
    // would never have returned.
    const q = new URLSearchParams(params)
    const levels = (q.get('tier') ?? '').split(',').filter(Boolean)
    const arms = levels.length
      ? data.segments.filter((s) => levels.includes(s.tier))
      : data.segments
    const merged = q.get('grouping') === 'single'
      ? [{
          ...arms[0], tier: 'ALL' as const,
          total: arms.reduce((n, s) => n + s.total, 0),
          target: arms.reduce((n, s) => n + s.target, 0),
          holdout: arms.reduce((n, s) => n + s.holdout, 0),
        }]
      : arms
    // The server says what each group brings; the fake answers one share per
    // group it was sent. A count of 92 orders matches nobody, as on prod.
    const extra = q.has('or_groups') ? JSON.parse(q.get('or_groups')!).length : 0
    const nobody = Number(q.get('orders_min')) > 20
    const groups = Array.from({ length: extra + 1 }, (_, i) => ({
      group: i + 1, matched: nobody && i === 0 ? 0 : 3000 + i,
      recipients: nobody && i === 0 ? 0 : 2000 + i, onlyHere: 500 + i,
    }))
    return {
      ...data,
      segments: merged,
      groups,
      totals: {
        customers: merged.reduce((n, s) => n + s.total, 0),
        target: merged.reduce((n, s) => n + s.target, 0),
        holdout: merged.reduce((n, s) => n + s.holdout, 0),
      },
    }
}

// The previews the page asks besides the audience's own — template counts,
// which condition emptied a group — answered by the same fake server, and not
// recorded in `segmentParams`, which is the audience's own query.
const countParams: string[] = []

vi.mock('../../hooks/useApi', () => ({
  useSmsSegments: (params: string) => {
    segmentParams.push(params)
    return { data: answer(params), isLoading: false, error: null, refetch: vi.fn() }
  },
  useSmsAudienceCounts: (items: Array<{ key: string; params: string | null }>) =>
    Object.fromEntries(items.filter((i) => i.params != null).map((i) => {
      countParams.push(i.params!)
      return [i.key, { status: 'done', data: answer(i.params!) }]
    })),
  useSmsCampaigns: () => ({ data: { campaigns: campaignList } }),
  useSmsAudiencePresets: () => ({
    data: {
      presets: [
        { name: 'RFM tiers', builtin: true, createdBy: null, createdAt: null, updatedAt: null,
          criteria: { grouping: 'rfm', ltvBasis: 'margin', maxRecencyDays: 270,
                      coreMinOrders: 2, reactivationMaxRecency: 120, salesType: 'retail',
                      holdoutPct: 10, filters: {} } },
        { name: 'Anua buyers', builtin: false, createdBy: 1,
          createdAt: null, updatedAt: null,
          criteria: { grouping: 'single', holdoutPct: 30,
                      filters: { brands: ['Anua'] } } },
      ],
    },
  }),
  useSaveSmsAudiencePreset: () => ({ mutate: savePresetMutate, isPending: false }),
  useDeleteSmsAudiencePreset: () => ({ mutate: vi.fn(), isPending: false }),
  useCreateSmsCampaign: () => ({ mutate: createMutate, isPending: false }),
  useBrands: () => ({ data: [{ name: 'Anua' }, { name: 'Medi-Peel' }] }),
  useCategories: () => ({ data: [{ id: 10, name: 'Face care' }] }),
  useSmsChannels: () => ({
    data: { sms: true, viber: false, smsSender: 'KoreanStory', viberSender: null,
            pricePerPart: 1.28 },
  }),
  useSendTestSms: () => ({ mutate: vi.fn(), isPending: false }),
  useSendSmsCampaign: () => ({ mutate: vi.fn(), isPending: false }),
  useSmsBalance: () => ({
    data: { sms: true, viber: false, smsSender: 'KoreanStory', viberSender: null,
            pricePerPart: 1.28, balance: 50_000 },
    isLoading: false,
  }),
}))

vi.mock('../Toast', () => ({ useToast: () => ({ addToast: vi.fn() }) }))

beforeEach(() => {
  segmentParams.length = 0
  createMutate.mockClear()
  savePresetMutate.mockClear()
  // The draft outlives a closed wizard on purpose, so it outlives a test too.
  sessionStorage.clear()
})

/** The parameters of the most recent preview query. */
function lastPreview(): URLSearchParams {
  return new URLSearchParams(segmentParams[segmentParams.length - 1])
}

/** The `or_groups` of the most recent preview, parsed, or null. */
function orGroups(): Array<Record<string, unknown>> | null {
  const raw = lastPreview().get('or_groups')
  return raw ? JSON.parse(raw) : null
}

async function step(name: string | RegExp) {
  await userEvent.click(screen.getByRole('button', { name }))
}

/** Step 1's "Next" carries the number of recipients; every other one does not. */
function nextButton(): HTMLElement {
  return screen.getByRole('button', { name: /^sms\.wizardNext/ })
}
/** Next waits for the count of the audience on screen — after any edit, the
 *  debounce and the request — so the click waits for it too. */
async function next() {
  await waitFor(() => expect(nextButton().hasAttribute('disabled')).toBe(false),
                { timeout: 2000 })
  await userEvent.click(nextButton())
}

/** Load a saved audience from the list. */
async function pickAudience(name: string) {
  await userEvent.selectOptions(
    screen.getByRole('combobox', { name: /sms\.presetsLabel/ }), name,
  )
}

/** Replace whatever the field holds — it opens with a suggested name. */
async function nameCampaign(name = 'sep-brand') {
  const field = screen.getByRole('textbox', { name: /sms\.campaignName/ })
  await userEvent.clear(field)
  await userEvent.type(field, name)
}

function nameField(): HTMLInputElement {
  return screen.getByRole('textbox', { name: /sms\.campaignName/ }) as HTMLInputElement
}

/** Group N (1-based), as the card a screen reader announces. */
function groupCard(n: number): HTMLElement {
  return screen.getByRole('region', { name: new RegExp(`sms\\.groupTitle\\(\\{"n":${n}\\}\\)`) })
}

/** "+ Condition" in a group, then the condition from the menu. */
async function addCondition(type: string, n = 1) {
  await userEvent.click(within(groupCard(n)).getByRole('button', { name: 'sms.cond.add' }))
  await userEvent.click(screen.getByRole('button', { name: new RegExp(`^sms\\.cond\\.menu\\.${type}`) }))
}

/** Type a number into the field labelled `label`, replacing what it held. */
async function typeNumber(label: string, value: string, scope: HTMLElement = document.body) {
  const field = within(scope).getByLabelText(label)
  await userEvent.clear(field)
  await userEvent.type(field, value)
}

/** Fill the walk to the freeze: control → message → launch → create. */
async function freeze() {
  await next()                          // → control
  await next()                          // → message
  await userEvent.type(screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка')
  await next()                          // → launch
  await step('sms.createCampaign')
}

/** Step 2: turn on the per-level measurement. */
async function useValueTiers() {
  await userEvent.click(screen.getByRole('checkbox'))
}

/** Click a tier chip. Chips carry their size, so match on the name only. */
async function pickTier(tier: string) {
  await userEvent.click(
    screen.getByRole('button', { name: new RegExp(`sms\\.tier\\.${tier}`) }),
  )
}

describe('SmsCampaignWizard', () => {
  it('previews one arm of everybody, over all time, before anything is touched', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    // Not the tier cascade: it would drop whoever matches none of its three
    // conditions, which no default should do. And no hidden window either.
    await waitFor(() => expect(lastPreview().get('grouping')).toBe('single'), { timeout: 2000 })
    expect(lastPreview().get('max_recency_days')).toBe('3650')
    expect(lastPreview().has('brand')).toBe(false)
    // An empty group is every buyer, and it says so.
    expect(within(groupCard(1)).getByText('sms.group.empty')).toBeTruthy()
  })

  it('applies a saved audience to the live preview, as conditions', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickAudience('Anua buyers')

    await waitFor(() => expect(lastPreview().get('brand')).toBe('Anua'), { timeout: 2000 })
    const p = lastPreview()
    expect(p.get('grouping')).toBe('single')
    expect(p.get('holdout_pct')).toBe('30')
    // A preset from before conditions had a 270-day window; it is now a
    // condition anybody can see and remove.
    expect(p.get('max_recency_days')).toBe('270')
    expect(within(groupCard(1)).getByRole('group', { name: 'sms.cond.label.lastOrder' })).toBeTruthy()
    expect(within(groupCard(1)).getByRole('group', { name: 'sms.cond.label.bought' })).toBeTruthy()
  })

  it('reproduces the built-in "RFM tiers" exactly', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickAudience('RFM tiers')
    await waitFor(() => expect(lastPreview().get('grouping')).toBe('rfm'), { timeout: 2000 })
    const p = Object.fromEntries(lastPreview())
    delete p.campaign
    expect(p).toEqual({
      ltv_basis: 'margin', holdout_pct: '10', grouping: 'rfm', max_recency_days: '270',
    })
  })

  it('saves the audience currently on screen under a name, conditions included', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickAudience('Anua buyers')
    await step('sms.presetSaveToggle')
    await userEvent.type(
      screen.getByRole('textbox', { name: /sms\.presetSaveAs/ }), 'My list',
    )
    await step('sms.presetSave')

    const saved = savePresetMutate.mock.calls[0][0]
    expect(saved).toMatchObject({
      name: 'My list',
      criteria: { grouping: 'single', filters: { brands: ['Anua'] } },
    })
    expect(saved.criteria.groups[0].conditions.map((c: { type: string }) => c.type))
      .toEqual(['lastOrder', 'bought'])
  })

  it('freezes exactly the audience that was previewed', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickAudience('Anua buyers')
    await nameCampaign()
    await next()                          // → control
    await userEvent.click(screen.getByRole('radio', { name: '30%' }))
    await next()                          // → message
    await userEvent.type(screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка')
    await next()                          // → launch
    await step('sms.createCampaign')

    const sent = new URLSearchParams(createMutate.mock.calls[0][0])
    expect(sent.get('campaign')).toBe('sep-brand')
    await waitFor(() => expect(lastPreview().get('campaign')).toBe('sep-brand'), { timeout: 2000 })
    // The preview and the freeze must not diverge: same audience, same name,
    // same query — so the same control group.
    expect(Object.fromEntries(sent)).toEqual(Object.fromEntries(lastPreview()))
  })

  it('will not leave the first step without a usable name', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    // It opens with a suggested name; take it away to see the guard.
    await userEvent.clear(nameField())
    expect(nextButton().hasAttribute('disabled')).toBe(true)
    expect(screen.getByText('sms.blocked.name')).toBeTruthy()

    // A name the API would reject is no better than none.
    await userEvent.type(nameField(), 'sep brand!')
    expect(nextButton().hasAttribute('disabled')).toBe(true)
    expect(screen.getByText('sms.campaignRequired')).toBeTruthy()

    await nameCampaign()
    await waitFor(() => expect(nextButton().hasAttribute('disabled')).toBe(false),
                  { timeout: 2000 })
    expect(createMutate).not.toHaveBeenCalled()
  })
})

describe('the control group on screen is the one that will be frozen', () => {
  it('previews under the campaign name, not under a placeholder', async () => {
    // On 2026-10-17 the preview drew the holdout under "default": 375 shown,
    // 429 frozen for the same audience.
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign('oct-brand')
    await waitFor(() => expect(lastPreview().get('campaign')).toBe('oct-brand'), { timeout: 2000 })

    // A name the server would refuse is left out rather than sent.
    await userEvent.type(nameField(), ' !')
    await waitFor(() => expect(lastPreview().has('campaign')).toBe(false), { timeout: 2000 })
  })

  it('shows the frozen numbers once the campaign is created', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign()
    await freeze()

    // The strip and the launch step both switch to the roster as frozen.
    expect(screen.getByText('sms.summaryFrozen')).toBeTruthy()
    expect(screen.getByText(/sms\.summaryContacts.*"target":"901".*"holdout":"99"/)).toBeTruthy()
    expect(screen.queryByText(/sms\.summaryContacts.*"target":"7,560"/)).toBeNull()
  })
})

describe('building an audience from conditions', () => {
  it('starts from a template, which opens as ordinary editable conditions', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await step(/^sms\.tpl\.repeat/)
    await waitFor(() => expect(lastPreview().get('orders_min')).toBe('2'), { timeout: 2000 })
    const row = within(groupCard(1)).getByRole('group', { name: 'sms.cond.label.orders' })
    // Its blanks open the editor in place.
    await userEvent.click(within(row).getAllByRole('button', { name: /sms\.cond\.change/ })[0])
    expect(within(row).getByLabelText('sms.cond.edit.count')).toBeTruthy()
  })

  it('keeps the audience a template replaced one tap away', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await step(/^sms\.tpl\.repeat/)
    await waitFor(() => expect(lastPreview().get('orders_min')).toBe('2'), { timeout: 2000 })
    // Nothing was there to lose: no offer.
    expect(screen.queryByRole('button', { name: 'sms.tpl.undo' })).toBeNull()

    await step(/^sms\.tpl\.lost/)
    await waitFor(() => expect(lastPreview().get('recency_min')).toBe('240'), { timeout: 2000 })
    await step('sms.tpl.undo')
    await waitFor(() => expect(lastPreview().get('orders_min')).toBe('2'), { timeout: 2000 })
    expect(lastPreview().has('recency_min')).toBe(false)
  })

  it('adds a condition from the menu, edits it in place, and removes it', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await addCondition('orders')
    // Inserted with its defaults and open for editing.
    await waitFor(() => expect(lastPreview().get('orders_min')).toBe('2'), { timeout: 2000 })
    await typeNumber('sms.cond.edit.count', '3')
    await waitFor(() => expect(lastPreview().get('orders_min')).toBe('3'), { timeout: 2000 })

    await userEvent.selectOptions(screen.getByLabelText('sms.cond.edit.op'), 'exactly')
    await waitFor(() => expect(lastPreview().get('orders_max')).toBe('3'), { timeout: 2000 })
    await step('sms.cond.done')
    expect(screen.queryByLabelText('sms.cond.edit.count')).toBeNull()

    await step(/sms\.cond\.remove/)
    await waitFor(() => expect(lastPreview().has('orders_min')).toBe(false), { timeout: 2000 })
    expect(within(groupCard(1)).getByText('sms.group.empty')).toBeTruthy()
  })

  it('says lifetime and in-period orders as one condition with a period', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await addCondition('orders')
    await userEvent.click(screen.getByRole('radio', { name: 'periodControl.last' }))
    await typeNumber('periodControl.days', '92')

    await waitFor(() => expect(lastPreview().get('period_days')).toBe('92'), { timeout: 2000 })
    const p = lastPreview()
    expect(p.get('period_orders_min')).toBe('2')
    expect(p.has('orders_min')).toBe(false)
    // All time is still a window of all time: nothing cuts the period.
    expect(p.get('max_recency_days')).toBe('3650')
  })

  it('offers a lifetime count and a period count side by side, never two of one kind', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await addCondition('orders')
    await step('sms.cond.done')
    await addCondition('orders')
    // The second takes the period the first left free.
    await waitFor(() => expect(lastPreview().get('period_days')).toBe('90'), { timeout: 2000 })
    expect(lastPreview().get('orders_min')).toBe('2')
    expect((screen.getByRole('radio', { name: 'periodControl.all' }) as HTMLButtonElement).disabled)
      .toBe(true)

    // A third has nowhere to go: the menu says why instead of hiding it.
    await step('sms.cond.done')
    await userEvent.click(within(groupCard(1)).getByRole('button', { name: 'sms.cond.add' }))
    expect((screen.getByRole('button', { name: /^sms\.cond\.menu\.orders/ }) as HTMLButtonElement)
      .disabled).toBe(true)
  })

  it('takes a period with both ends, and swaps a reversed one rather than send it', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await addCondition('orders')
    await userEvent.click(screen.getByRole('radio', { name: 'periodControl.range' }))
    fireEvent.change(screen.getByLabelText('periodControl.from'), { target: { value: '2026-10-14' } })
    // Typed past the picker's own limit: the end before the start.
    fireEvent.change(screen.getByLabelText('periodControl.to'), { target: { value: '2026-01-01' } })

    expect(screen.getByText('periodControl.swapped')).toBeTruthy()
    await waitFor(() => expect(lastPreview().get('period_to')).toBe('2026-10-14'), { timeout: 2000 })
    expect(lastPreview().get('period_from')).toBe('2026-01-01')
    // The two native inputs bound each other, so a picker never offers it.
    expect(screen.getByLabelText('periodControl.to').getAttribute('min')).toBe('2026-01-01')
    expect(screen.getByLabelText('periodControl.from').getAttribute('max')).toBe('2026-10-14')
  })

  it('catches 92 typed as an order count and moves it into the period in one tap', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await addCondition('orders')
    await typeNumber('sms.cond.edit.count', '92')
    await waitFor(() => expect(lastPreview().get('orders_min')).toBe('92'), { timeout: 2000 })
    // The fake, like prod, matches nobody with 92 orders.
    await waitFor(() => expect(screen.getByText(/sms\.cond\.manyOrdersNobody/)).toBeTruthy(),
                  { timeout: 2000 })
    // The card names the condition that did it — the group's only one, so
    // without asking anything more — in place of "nobody matches".
    expect(screen.getByText(/sms\.groupWhy\.alone\(\{"condition":"sms\.cond\.label\.orders /))
      .toBeTruthy()
    expect(screen.queryByText('sms.groupMatchesNobody')).toBeNull()

    await step(/sms\.cond\.makeItDays/)
    await waitFor(() => expect(lastPreview().get('period_days')).toBe('92'), { timeout: 2000 })
    expect(lastPreview().get('period_orders_min')).toBe('1')
    expect(lastPreview().has('orders_min')).toBe(false)
  })

  it('does not offer to move 92 into a period the group already has', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await step(/^sms\.tpl\.recent/)               // 1+ order in the last 30 days
    await addCondition('orders')                   // the lifetime count beside it
    await typeNumber('sms.cond.edit.count', '92')
    await waitFor(() => expect(screen.getByText(/sms\.cond\.manyOrders/)).toBeTruthy(),
                  { timeout: 2000 })
    // A second period would be refused by the server, or replace the first.
    expect(screen.queryByRole('button', { name: /sms\.cond\.makeItDays/ })).toBeNull()
  })

  it('leaves an unfinished condition out, says so, and holds the step', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await addCondition('bought')
    expect(screen.getByText('sms.cond.problem.incomplete')).toBeTruthy()
    expect(screen.getByText(/sms\.blocked\.unfinished\(\{"groups":"1"\}\)/)).toBeTruthy()
    expect(nextButton().hasAttribute('disabled')).toBe(true)
    expect(lastPreview().has('brand')).toBe(false)

    await userEvent.click(screen.getByRole('button', { name: 'Anua' }))
    await waitFor(() => expect(lastPreview().get('brand')).toBe('Anua'), { timeout: 2000 })
    await waitFor(() => expect(nextButton().hasAttribute('disabled')).toBe(false),
                  { timeout: 2000 })
  })

  it('reads the summary from the definition, with the count', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await step(/^sms\.tpl\.lost/)
    await waitFor(() => expect(
      screen.getAllByText(/^sms\.sum\.withCount/)[0].textContent,
    ).toMatch(/sms\.sum\.lastOrder/), { timeout: 2000 })
  })
})

describe('the value level, for the whole campaign', () => {
  it('stays a filter in step 1, whatever the measurement', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await waitFor(() => expect(lastPreview().get('grouping')).toBe('single'), { timeout: 2000 })
    await pickTier('VIP')
    await waitFor(() => expect(lastPreview().get('tier')).toBe('VIP'), { timeout: 2000 })

    // Switching to a per-level measurement, now in step 2, does not change
    // who was picked.
    await next()
    await useValueTiers()
    await waitFor(() => expect(lastPreview().get('tier')).toBe('VIP'), { timeout: 2000 })
    await waitFor(() => expect(lastPreview().get('grouping')).toBe('rfm'), { timeout: 2000 })
  })

  it('freezes the levels that were picked', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickTier('VIP')
    await pickTier('CORE')
    await nameCampaign()
    await freeze()

    expect(new URLSearchParams(createMutate.mock.calls[0][0]).get('tier')).toBe('VIP,CORE')
  })
})

describe('step 2: how the campaign is measured', () => {
  it('holds the split, the thresholds and the basis — none of them in step 1', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    expect(screen.queryByRole('checkbox')).toBeNull()
    expect(screen.queryByText('sms.measureSplit')).toBeNull()
    await next()
    expect(screen.getByText('sms.measureSplit')).toBeTruthy()
    await userEvent.click(screen.getByRole('button', { name: 'sms.measure.advanced' }))
    expect(screen.getByLabelText('sms.basisLabel')).toBeTruthy()
  })

  it('counts only the picked arms towards the campaign', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickTier('VIP')
    await next()
    await useValueTiers()

    // VIP alone: 900 to send and 100 withheld, not the 7,560 of all three.
    await waitFor(() => expect(
      screen.getByText(/sms\.holdoutHint.*"target":"900".*"holdout":"100"/),
    ).toBeTruthy(), { timeout: 2000 })
  })

  it('shows each arm its own size on the level chip', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)
    await next()
    await useValueTiers()
    await step('sms.wizardBack')

    // The sizes are the whole basis for deciding which arms to include.
    await waitFor(() => expect(
      screen.getByRole('button', { name: /sms\.tier\.VIP · 900/ }),
    ).toBeTruthy(), { timeout: 2000 })
    expect(screen.getByRole('button', { name: /sms\.tier\.CORE · 2,700/ })).toBeTruthy()
  })

  it('states the threshold per arm, and calls a hopeless split hopeless', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)
    await next()

    // One arm of 7,560 against 840 sees a lift from 1.60 pp — close to the
    // ~2 pp the only campaign there has been produced, so: tight.
    await waitFor(() => expect(screen.getByText('sms.mdeTight')).toBeTruthy(), { timeout: 2000 })

    await useValueTiers()
    // Split three ways, the smallest arm is 900 against 100.
    await waitFor(() => expect(screen.getByText('sms.mdeHopeless')).toBeTruthy(),
                  { timeout: 2000 })
  })

  it('sharpens as the control share grows', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)
    await nameCampaign()
    await next()

    await userEvent.click(screen.getByRole('radio', { name: '40%' }))
    await waitFor(() => expect(lastPreview().get('holdout_pct')).toBe('40'), { timeout: 2000 })
  })
})

describe('the rehearsal', () => {
  it('opens with the text that is about to be sent', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign()
    await next()
    await next()
    await userEvent.type(
      screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка 30%',
    )
    await step('sms.testSend')

    const boxes = screen.getAllByRole('textbox')
      .filter((el) => (el as HTMLTextAreaElement).value === 'Знижка 30%')
    expect(boxes.length).toBeGreaterThan(1)
  })
})

describe('the step footer', () => {
  it('puts the step action last on every step, and back first', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    function footerOrder(): string[] {
      const action = screen.getByRole('button', { name: /^sms\.(wizardNext|createCampaign)/ })
      const footer = action.closest('div')!
      return Array.from(footer.querySelectorAll('button')).map((b) => b.textContent ?? '')
    }

    // Step 1: nothing but the action, in the audience bar.
    expect(footerOrder()).toHaveLength(1)
    expect(footerOrder()[0]).toMatch(/^sms\.wizardNext/)

    await nameCampaign()
    await next()
    // Step 2: back first, action last.
    expect(footerOrder()[0]).toBe('sms.wizardBack')
    expect(footerOrder().at(-1)).toBe('sms.wizardNext')

    await next()
    // Step 3: back first, the rehearsal between, action last.
    expect(footerOrder()[0]).toBe('sms.wizardBack')
    expect(footerOrder()).toContain('sms.testSend')
    expect(footerOrder().at(-1)).toBe('sms.wizardNext')
  })

  it('says how many it will go to on the way out of step 1', () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)
    expect(nextButton().textContent).toMatch(/sms\.wizardNextCount\(\{"n":"7,560"/)
  })

  it('holds Next, and says it is recounting, while the numbers are the last audience\'s',
     async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)
    await waitFor(() => expect(nextButton().hasAttribute('disabled')).toBe(false),
                  { timeout: 2000 })

    await addCondition('orders')
    expect(nextButton().hasAttribute('disabled')).toBe(true)
    expect(screen.getByText('sms.bar.recounting')).toBeTruthy()
    await waitFor(() => expect(nextButton().hasAttribute('disabled')).toBe(false),
                  { timeout: 2000 })
  })
})

describe('the saved-audience list', () => {
  it('stops claiming to describe the audience once it is edited', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickAudience('Anua buyers')
    const list = screen.getByRole('combobox', { name: /sms\.presetsLabel/ })
    expect((list as HTMLSelectElement).value).toBe('Anua buyers')

    await addCondition('orders')
    expect((list as HTMLSelectElement).value).toBe('')
  })

  it('offers deletion only for a saved audience, never a built-in', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickAudience('RFM tiers')
    expect(screen.queryByRole('button', { name: 'sms.presetDeleteSelected' })).toBeNull()

    await pickAudience('Anua buyers')
    expect(screen.getByRole('button', { name: 'sms.presetDeleteSelected' })).toBeTruthy()
  })
})

describe('like a past campaign', () => {
  it('loads how its people were chosen, not its roster', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await step(/sms\.library\.likePast/)
    // Follow-ups have no audience of their own and are not offered.
    expect(screen.queryByRole('button', { name: /^oct-promo-noon/ })).toBeNull()
    await step(/^oct-promo/)

    await waitFor(() => expect(lastPreview().get('period_from')).toBe('2026-01-01'), { timeout: 2000 })
    expect(lastPreview().get('period_to')).toBe('2026-10-14')
    expect(lastPreview().get('period_orders_min')).toBe('2')
  })
})

describe('the draft', () => {
  it('survives closing the wizard, conditions and all', async () => {
    const { unmount } = render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign('sep-brand')
    await pickTier('VIP')
    await addCondition('orders')
    await typeNumber('sms.cond.edit.count', '4')
    unmount()

    render(<SmsCampaignWizard onClose={vi.fn()} />)
    expect(nameField().value).toBe('sep-brand')
    expect(
      screen.getByRole('button', { name: /sms\.tier\.VIP/ }).getAttribute('aria-pressed'),
    ).toBe('true')
    await waitFor(() => expect(lastPreview().get('orders_min')).toBe('4'), { timeout: 2000 })
  })

  it('is cleared once the roster is frozen', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign()
    await freeze()

    expect(sessionStorage.getItem('sms.campaignDraft.v2')).toBeNull()
    expect(sessionStorage.getItem('sms.campaignDraft.v1')).toBeNull()
  })

  it('opens a draft written before conditions, as conditions', async () => {
    sessionStorage.setItem('sms.campaignDraft.v1', JSON.stringify({
      campaign: 'v1-draft',
      audience: { grouping: 'single', ltvBasis: 'margin', holdoutPct: 10, tiers: [], tierRules: {},
                  groups: [{ maxRecencyDays: 92, filters: { ordersMin: 2 } }] },
    }))
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    expect(nameField().value).toBe('v1-draft')
    await waitFor(() => expect(lastPreview().get('max_recency_days')).toBe('92'), { timeout: 2000 })
    expect(lastPreview().get('orders_min')).toBe('2')
  })
})

describe('the cost estimate', () => {
  it('prices the send at the gateway tariff, once there is a text', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    // No text yet: one part per recipient is the floor, and the line says so.
    expect(screen.getByText(/sms\.summaryCostFrom/)).toBeTruthy()

    await nameCampaign()
    await next()
    await next()
    await userEvent.type(
      screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка',
    )

    expect(screen.getByText(/sms\.summaryCost\(/)).toBeTruthy()
  })
})

describe('the send dialog', () => {
  it('opens with the text the campaign was written and priced with', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign()
    await next()
    await next()
    await userEvent.type(
      screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка 30%',
    )
    await next()
    await step('sms.createCampaign')
    await step('sms.sendNow')

    const filled = screen.getAllByRole('textbox')
      .filter((el) => (el as HTMLTextAreaElement).value === 'Знижка 30%')
    expect(filled.length).toBeGreaterThan(0)
  })
})

// ─── One numbering on the page ──────────────────────────────────────────────

describe('SmsCampaignWizard, in the page it belongs to', () => {
  it('is step one of the page, and says so', () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)
    expect(screen.getByText(/sms\.step.*"n":1/)).toBeInTheDocument()
  })

  it('numbers none of its own stages', () => {
    const { container } = render(<SmsCampaignWizard onClose={vi.fn()} />)
    const nav = container.querySelector('nav')!
    expect(nav.textContent).not.toMatch(/[1-4]/)
  })
})

describe('the suggested name', () => {
  it('opens filled with a name nobody has used, and says it was suggested', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    const expected = suggestCampaignName(campaignList.map((c) => c.campaign))
    expect(expected).toMatch(/^sms-\d{4}-\d{2}-\d{2}$/)
    expect(nameField().value).toBe(expected)
    expect(screen.getByText('sms.campaignNameSuggested')).toBeTruthy()
    await waitFor(() => expect(nextButton().hasAttribute('disabled')).toBe(false),
                  { timeout: 2000 })
  })

  it('is the followed campaign plus -push for a follow-up', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await userEvent.click(screen.getByRole('radio', { name: 'sms.sourceFollowUp' }))
    await userEvent.selectOptions(
      screen.getByRole('combobox', { name: /sms\.followUpPick/ }), 'oct-promo',
    )
    expect(nameField().value).toBe('oct-promo-push')
  })

  it('never replaces a name somebody typed', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign('oct-mine')
    expect(screen.queryByText('sms.campaignNameSuggested')).toBeNull()
    await userEvent.click(screen.getByRole('radio', { name: 'sms.sourceFollowUp' }))
    await userEvent.selectOptions(
      screen.getByRole('combobox', { name: /sms\.followUpPick/ }), 'oct-promo',
    )
    expect(nameField().value).toBe('oct-mine')
  })
})

describe('a follow-up', () => {
  async function followUp(root = 'oct-promo') {
    await userEvent.click(screen.getByRole('radio', { name: 'sms.sourceFollowUp' }))
    await userEvent.selectOptions(
      screen.getByRole('combobox', { name: /sms\.followUpPick/ }), root,
    )
  }

  it('offers only sent campaigns of their own from the last week', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await userEvent.click(screen.getByRole('radio', { name: 'sms.sourceFollowUp' }))

    const options = Array.from(
      (screen.getByRole('combobox', { name: /sms\.followUpPick/ }) as HTMLSelectElement)
        .options,
    ).map((o) => o.value).filter(Boolean)
    expect(options).toEqual(['oct-promo'])
  })

  it('previews nothing but the campaign it follows, and shows no conditions', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await followUp()

    expect([...lastPreview().keys()]).toEqual(['follow_up_of'])
    expect(lastPreview().get('follow_up_of')).toBe('oct-promo')
    // The audience builder is gone: there is nothing of it to narrow.
    expect(screen.queryByText('sms.tpl.title')).toBeNull()
    expect(screen.queryByRole('button', { name: 'sms.cond.add' })).toBeNull()
    expect(screen.getByText(/sms\.followUpRecipients/)).toBeTruthy()
  })

  it('skips the control step and freezes exactly the previewed follow-up', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign('oct-last-day')
    await followUp()
    expect(screen.queryByRole('button', { name: 'sms.wizard.control' })).toBeNull()
    await next()
    expect(screen.queryByText('sms.holdoutExplain')).toBeNull()
    await userEvent.type(
      screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Останній день',
    )
    await next()
    await step('sms.createCampaign')

    const sent = new URLSearchParams(createMutate.mock.calls[0][0])
    expect(Object.fromEntries(sent)).toEqual({
      follow_up_of: 'oct-promo', campaign: 'oct-last-day',
    })
  })

  it('goes back from the message to the follow-up, not to a control step', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign('oct-last-day')
    await followUp()
    await next()
    await step('sms.wizardBack')

    expect(screen.getByRole('combobox', { name: /sms\.followUpPick/ })).toBeTruthy()
  })
})

describe('audience groups, ORed', () => {
  it('starts as one group, which can never be removed', () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    expect(groupCard(1)).toBeTruthy()
    expect(screen.queryByRole('button', { name: /sms\.groupRemove/ })).toBeNull()
    // "+ another group" appears once group 1 says something.
    expect(screen.queryByRole('button', { name: 'sms.groupAdd' })).toBeNull()
  })

  it('adds a group ORed with the first, sent as one parameter', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await addCondition('orders')
    await step('sms.cond.done')
    await step('sms.groupAdd')
    expect(screen.getByRole('separator', { name: 'sms.groupsOrLabel' })).toBeTruthy()
    await addCondition('lastOrder', 2)

    await waitFor(() => expect(orGroups()).toEqual([{ max_recency_days: 90 }]), { timeout: 2000 })
    // The first group is still the flat parameters, untouched by the second.
    expect(lastPreview().get('orders_min')).toBe('2')
    expect(lastPreview().get('max_recency_days')).toBe('3650')
  })

  it('removes an extra group, and the audience is one group again', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await addCondition('orders')
    await step('sms.cond.done')
    await step('sms.groupAdd')
    await waitFor(() => expect(orGroups()).not.toBeNull(), { timeout: 2000 })
    await userEvent.click(screen.getByRole('button', { name: /sms\.groupRemove\(\{"n":2\}\)/ }))
    await waitFor(() => expect(orGroups()).toBeNull(), { timeout: 2000 })
  })

  it('duplicates a group to edit a variant of it', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await step(/^sms\.tpl\.repeat/)
    await step(/sms\.group\.duplicate\(\{"n":1\}\)/)
    await waitFor(() => expect(orGroups()).toEqual([{ max_recency_days: 3650, orders_min: 2 }]),
                  { timeout: 2000 })
  })

  it("says the owner's campaign: two orders in one period, or one in another", async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    async function ordersIn(n: number, count: string, from: string, to: string) {
      await addCondition('orders', n)
      const card = groupCard(n)
      await typeNumber('sms.cond.edit.count', count, card)
      await userEvent.click(within(card).getByRole('radio', { name: 'periodControl.range' }))
      fireEvent.change(within(card).getByLabelText('periodControl.from'), { target: { value: from } })
      fireEvent.change(within(card).getByLabelText('periodControl.to'), { target: { value: to } })
      await userEvent.click(within(card).getByRole('button', { name: 'sms.cond.done' }))
    }

    await ordersIn(1, '2', '2026-01-01', '2026-10-14')
    await step('sms.groupAdd')
    await ordersIn(2, '1', '2026-07-16', '2026-10-14')

    await waitFor(() => expect(orGroups()).toEqual([{
      max_recency_days: 3650, period_orders_min: 1,
      period_from: '2026-07-16', period_to: '2026-10-14',
    }]), { timeout: 2000 })
    const p = lastPreview()
    expect(p.get('max_recency_days')).toBe('3650')
    expect(p.get('period_orders_min')).toBe('2')
    expect(p.get('period_from')).toBe('2026-01-01')
    expect(p.get('period_to')).toBe('2026-10-14')
    expect(p.has('orders_min')).toBe(false)

    await nameCampaign()
    await freeze()
    const sent = new URLSearchParams(createMutate.mock.calls[0][0])
    expect(sent.get('or_groups')).toBe(p.get('or_groups'))
    expect(sent.get('period_to')).toBe('2026-10-14')
  })

  it('shows what each group brings, and how many several groups share', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await addCondition('orders')
    await step('sms.cond.done')
    await step('sms.groupAdd')
    // An empty second group is every buyer: the step says so and holds.
    expect(screen.getByText(/sms\.blocked\.emptyGroup\(\{"groups":"2"\}\)/)).toBeTruthy()
    await addCondition('lastOrder', 2)
    await waitFor(() => expect(
      within(groupCard(1)).getByText(/sms\.groupShare\(\{"recipients":"2\D?000"/),
    ).toBeTruthy(), { timeout: 2000 })
    // 8,400 in the audience, 500 + 501 only in one group: 7,399 in both.
    await waitFor(() => expect(screen.getByText(/sms\.bar\.overlap\(\{"n":"7\D?399","word":"word\.fits"\}\)/)).toBeTruthy(),
                  { timeout: 2000 })
  })

  it('keeps the groups in the draft', async () => {
    const { unmount } = render(<SmsCampaignWizard onClose={vi.fn()} />)
    await addCondition('orders')
    await step('sms.cond.done')
    await step('sms.groupAdd')
    unmount()

    render(<SmsCampaignWizard onClose={vi.fn()} />)
    expect(groupCard(2)).toBeTruthy()
  })
})

describe('on a phone', () => {
  const original = window.matchMedia
  beforeEach(() => {
    window.matchMedia = ((query: string) => ({
      matches: query.includes('max-width'), media: query, onchange: null,
      addEventListener: () => {}, removeEventListener: () => {},
      addListener: () => {}, removeListener: () => {}, dispatchEvent: () => false,
    })) as unknown as typeof window.matchMedia
  })
  afterEach(() => { window.matchMedia = original })

  it('adds from a bottom sheet, edits in one sheet, and counts when it closes', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await userEvent.click(within(groupCard(1)).getByRole('button', { name: 'sms.cond.add' }))
    const menu = screen.getByRole('dialog', { name: 'sms.cond.addTitle' })
    await userEvent.click(within(menu).getByRole('button', { name: /^sms\.cond\.menu\.orders/ }))

    // The menu sheet is gone and the editor sheet is in its place — never two.
    expect(screen.queryByRole('dialog', { name: 'sms.cond.addTitle' })).toBeNull()
    const sheet = screen.getByRole('dialog', { name: 'sms.cond.label.orders' })
    await waitFor(() => expect(lastPreview().get('orders_min')).toBe('2'), { timeout: 2000 })

    await typeNumber('sms.cond.edit.count', '5', sheet)
    // Not yet: a phone recounts once, when the sheet closes.
    await new Promise((r) => setTimeout(r, 700))
    expect(lastPreview().get('orders_min')).toBe('2')

    await userEvent.click(within(sheet).getByRole('button', { name: 'sms.cond.done' }))
    expect(screen.queryByRole('dialog')).toBeNull()
    await waitFor(() => expect(lastPreview().get('orders_min')).toBe('5'), { timeout: 2000 })
  })
})

describe('inside the Telegram Mini App', () => {
  // Telegram's SDK as the wizard sees it: two native buttons that record what
  // the page asked of them, and hand back the listener the page registered.
  function fakeWebApp() {
    const button = () => {
      const listeners: Array<() => void> = []
      return {
        listeners,
        setParams: vi.fn(),
        show: vi.fn(),
        hide: vi.fn(),
        onClick: vi.fn((cb: () => void) => { listeners.push(cb) }),
        offClick: vi.fn((cb: () => void) => {
          const i = listeners.indexOf(cb)
          if (i >= 0) listeners.splice(i, 1)
        }),
        press: () => listeners.forEach((cb) => cb()),
      }
    }
    return {
      platform: 'ios',
      isVersionAtLeast: () => true,
      MainButton: button(),
      BackButton: button(),
    }
  }
  type Fake = ReturnType<typeof fakeWebApp>
  let tg: Fake
  const original = window.matchMedia

  beforeEach(() => {
    tg = fakeWebApp()
    ;(window as unknown as { TelegramWebviewProxy: object }).TelegramWebviewProxy = {}
    ;(window as unknown as { Telegram: object }).Telegram = { WebApp: tg }
  })
  afterEach(() => {
    delete (window as unknown as { TelegramWebviewProxy?: object }).TelegramWebviewProxy
    delete (window as unknown as { Telegram?: object }).Telegram
    window.matchMedia = original
  })

  const lastMain = () => tg.MainButton.setParams.mock.calls.at(-1)?.[0]

  it("is the bar's Next — same words, same state, same step — and the bar draws none", async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await waitFor(() => expect(lastMain()).toMatchObject({ is_active: true, is_visible: true }),
                  { timeout: 2000 })
    expect(lastMain().text).toMatch(/^sms\.wizardNextCount\(.*"n":"7,560"/)
    expect(screen.queryByRole('button', { name: /^sms\.wizardNext/ })).toBeNull()
    expect(tg.BackButton.show).not.toHaveBeenCalled()

    await act(async () => tg.MainButton.press())
    expect(screen.getByRole('button', { name: 'sms.wizardBack' })).toBeTruthy()
    expect(tg.MainButton.hide).toHaveBeenCalled()
    expect(tg.BackButton.show).toHaveBeenCalled()

    await act(async () => tg.BackButton.press())
    await waitFor(() => expect(lastMain()).toMatchObject({ is_visible: true }))
  })

  it('stays greyed while Next is held, and a press then does nothing', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)
    await waitFor(() => expect(lastMain()).toMatchObject({ is_active: true }), { timeout: 2000 })

    await userEvent.clear(screen.getByRole('textbox', { name: /sms\.campaignName/ }))
    await waitFor(() => expect(lastMain()).toMatchObject({ is_active: false }))
    await act(async () => tg.MainButton.press())
    expect(screen.queryByRole('button', { name: 'sms.wizardBack' })).toBeNull()
  })

  it('closes the sheet on top before it steps back, and hides itself for the sheet', async () => {
    window.matchMedia = ((query: string) => ({
      matches: query.includes('max-width'), media: query, onchange: null,
      addEventListener: () => {}, removeEventListener: () => {},
      addListener: () => {}, removeListener: () => {}, dispatchEvent: () => false,
    })) as unknown as typeof window.matchMedia
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await userEvent.click(within(groupCard(1)).getByRole('button', { name: 'sms.cond.add' }))
    expect(screen.getByRole('dialog', { name: 'sms.cond.addTitle' })).toBeTruthy()
    await waitFor(() => expect(tg.BackButton.show).toHaveBeenCalled())
    expect(tg.MainButton.hide).toHaveBeenCalled()

    await act(async () => tg.BackButton.press())
    expect(screen.queryByRole('dialog')).toBeNull()
    // Still on the audience: the press closed the sheet and nothing else.
    expect(screen.getByRole('region', { name: /sms\.groupTitle/ })).toBeTruthy()
  })

  it('hides both buttons and lets go of them when the wizard closes', async () => {
    const { unmount } = render(<SmsCampaignWizard onClose={vi.fn()} />)
    await waitFor(() => expect(lastMain()).toBeTruthy(), { timeout: 2000 })
    tg.MainButton.hide.mockClear()
    tg.BackButton.hide.mockClear()

    unmount()
    expect(tg.MainButton.hide).toHaveBeenCalled()
    expect(tg.BackButton.hide).toHaveBeenCalled()
    expect(tg.MainButton.listeners).toEqual([])
    expect(tg.BackButton.listeners).toEqual([])
  })
})

describe('outside Telegram', () => {
  it('draws its own Next and touches no SDK', () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)
    expect(screen.getByRole('button', { name: /^sms\.wizardNext/ })).toBeTruthy()
    expect(document.querySelector('script[src*="telegram"]')).toBeNull()
  })
})
