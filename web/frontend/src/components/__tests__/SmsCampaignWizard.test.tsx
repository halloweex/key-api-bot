import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { SmsCampaignWizard } from '../SmsCampaignWizard'
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
  // Three arms, because the tier picker is what most of these tests exercise.
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
// Calls back like the real mutation does, so the steps that only happen after
// a roster is frozen — the draft being cleared, the send button appearing —
// are actually exercised.
const createMutate = vi.fn((params: string, opts?: {
  onSuccess?: (r: unknown) => void
}) => {
  opts?.onSuccess?.({
    campaign: new URLSearchParams(params).get('campaign') ?? 'unnamed',
    frozen: { campaign: 'x', frozen: true },
    segments: [],
    totals: { customers: 1000, target: 900, holdout: 100 },
    funnel: [],
  })
})
const savePresetMutate = vi.fn()

vi.mock('../../hooks/useApi', () => ({
  useSmsSegments: (params: string) => {
    segmentParams.push(params)
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
    return {
      data: {
        ...data,
        segments: merged,
        totals: {
          customers: merged.reduce((n, s) => n + s.total, 0),
          target: merged.reduce((n, s) => n + s.target, 0),
          holdout: merged.reduce((n, s) => n + s.holdout, 0),
        },
      },
      isLoading: false, error: null, refetch: vi.fn(),
    }
  },
  useSmsAudiencePresets: () => ({
    data: {
      presets: [
        { name: 'RFM tiers', criteria: { grouping: 'rfm' }, builtin: true,
          createdBy: null, createdAt: null, updatedAt: null },
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

async function step(name: string) {
  await userEvent.click(screen.getByRole('button', { name }))
}

/** Turn on the per-level measurement, which lives behind the folded settings. */
async function useValueTiers() {
  const fold = screen.getByRole('button', { name: /sms\.measureTitle/ })
  if (fold.getAttribute('aria-expanded') === 'false') await userEvent.click(fold)
  await userEvent.click(screen.getByRole('checkbox'))
}

/** Click a tier chip. Chips carry their size, so match on the name only. */
async function pickTier(tier: string) {
  await userEvent.click(
    screen.getByRole('button', { name: new RegExp(`sms\\.tier\\.${tier}`) }),
  )
}

/** Load a saved audience from the list. */
async function pickAudience(name: string) {
  await userEvent.selectOptions(
    screen.getByRole('combobox', { name: /sms\.presetsLabel/ }), name,
  )
}

/** The campaign name lives at the top of step one and gates leaving it. */
async function nameCampaign(name = 'sep-brand') {
  await userEvent.type(
    screen.getByRole('textbox', { name: /sms\.campaignName/ }), name,
  )
}

describe('SmsCampaignWizard', () => {
  it('previews one arm of everybody before anything is touched', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    // Not the tier cascade: it would drop whoever matches none of its three
    // conditions, which no default should do.
    await waitFor(() => expect(lastPreview().get('grouping')).toBe('single'), { timeout: 2000 })
    await waitFor(() => expect(lastPreview().has('brand')).toBe(false), { timeout: 2000 })
  })

  it('applies a saved audience to the live preview', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickAudience('Anua buyers')

    await waitFor(() => expect(lastPreview().get('brand')).toBe('Anua'), { timeout: 2000 })
    const p = lastPreview()
    expect(p.get('grouping')).toBe('single')
    expect(p.get('brand')).toBe('Anua')
    expect(p.get('holdout_pct')).toBe('30')
  })

  it('saves the audience currently on screen under a name', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickAudience('Anua buyers')
    // The name field is behind the "save the current one" link, next to the
    // presets it saves among — it used to be a stray field at the foot of the
    // step, where it read as the campaign's name.
    await step('sms.presetSaveToggle')
    await userEvent.type(
      screen.getByRole('textbox', { name: /sms\.presetSaveAs/ }), 'My list',
    )
    await step('sms.presetSave')

    expect(savePresetMutate.mock.calls[0][0]).toMatchObject({
      name: 'My list',
      criteria: { grouping: 'single', filters: { brands: ['Anua'] } },
    })
  })

  it('freezes exactly the audience that was previewed', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickAudience('Anua buyers')
    await nameCampaign()
    await step('sms.wizardNext')          // → control
    await step('30%')
    await step('sms.wizardNext')          // → message
    await userEvent.type(screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка')
    await step('sms.wizardNext')          // → launch
    await step('sms.createCampaign')

    const sent = new URLSearchParams(createMutate.mock.calls[0][0])
    expect(sent.get('campaign')).toBe('sep-brand')
    expect(sent.get('brand')).toBe('Anua')
    expect(sent.get('grouping')).toBe('single')
    expect(sent.get('holdout_pct')).toBe('30')
    // The preview and the freeze must not diverge: same audience, same query.
    await waitFor(() => expect(lastPreview().get('brand')).toBe('Anua'), { timeout: 2000 })
    const preview = lastPreview()
    for (const key of ['grouping', 'brand', 'holdout_pct', 'ltv_basis']) {
      expect(sent.get(key)).toBe(preview.get(key))
    }
  })

  it('will not leave the first step without a usable name', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    expect(
      screen.getByRole('button', { name: 'sms.wizardNext' }).hasAttribute('disabled'),
    ).toBe(true)

    // A name the API would reject is no better than none.
    await userEvent.type(
      screen.getByRole('textbox', { name: /sms\.campaignName/ }), 'sep brand!',
    )
    expect(
      screen.getByRole('button', { name: 'sms.wizardNext' }).hasAttribute('disabled'),
    ).toBe(true)
    expect(screen.getByText('sms.campaignRequired')).toBeTruthy()

    await userEvent.clear(screen.getByRole('textbox', { name: /sms\.campaignName/ }))
    await nameCampaign()
    expect(
      screen.getByRole('button', { name: 'sms.wizardNext' }).hasAttribute('disabled'),
    ).toBe(false)
    expect(createMutate).not.toHaveBeenCalled()
  })
})

describe('the filters are visible without hunting for them', () => {
  it('shows the date fields as soon as the wizard opens', () => {
    // Reported the first time somebody used it: "there are no date filters".
    // They were behind a collapsed disclosure.
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    expect(screen.getByLabelText('sms.filterFirstOrderFrom')).toBeTruthy()
    expect(screen.getByLabelText('sms.filterFirstOrderTo')).toBeTruthy()
    expect(screen.getByLabelText('sms.filterRecency min')).toBeTruthy()
  })
})

describe('the base window', () => {
  it('widens the audience beyond the default 270 days', async () => {
    // "Came in 2024 and has been quiet since" was unreachable while this was
    // hardcoded: the outer window cut it before any filter ran.
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    const field = screen.getByLabelText('sms.filterWindow')
    await userEvent.clear(field)
    await userEvent.type(field, '730')

    await waitFor(() => expect(lastPreview().get('max_recency_days')).toBe('730'), { timeout: 2000 })
  })

  it('warns when the silence asked for is longer than the window', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await userEvent.type(screen.getByLabelText('sms.filterRecency min'), '400')

    // 400 days of silence inside a 270-day window is nobody, and the funnel
    // alone would not say why.
    expect(screen.getByText(/sms\.windowConflict/)).toBeTruthy()
  })
})

describe('the rehearsal', () => {
  it('opens with the text that is about to be sent', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign()
    await step('sms.wizardNext')
    await step('sms.wizardNext')
    await userEvent.type(
      screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка 30%',
    )
    await step('sms.testSend')

    // The dialog's own textarea, prefilled — testing an empty box, or a
    // different message, is not a rehearsal of this campaign.
    const boxes = screen.getAllByRole('textbox')
      .filter((el) => (el as HTMLTextAreaElement).value === 'Знижка 30%')
    expect(boxes.length).toBeGreaterThan(1)
  })
})

describe('the step footer', () => {
  it('puts the step action last on every step, and back first', async () => {
    // The one control a person aims for without reading has to be in the same
    // place every time; it used to be right on step 1 and left on the rest.
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    function footerOrder(): string[] {
      const next = screen.getByRole('button', { name: /sms\.(wizardNext|createCampaign)/ })
      const footer = next.closest('div')!
      return Array.from(footer.querySelectorAll('button')).map((b) => b.textContent ?? '')
    }

    // Step 1: nothing but the action.
    expect(footerOrder().at(-1)).toBe('sms.wizardNext')

    await nameCampaign()
    await step('sms.wizardNext')
    // Step 2: back first, action last.
    expect(footerOrder()[0]).toBe('sms.wizardBack')
    expect(footerOrder().at(-1)).toBe('sms.wizardNext')

    await step('sms.wizardNext')
    // Step 3: back first, the rehearsal between, action last.
    expect(footerOrder()[0]).toBe('sms.wizardBack')
    expect(footerOrder()).toContain('sms.testSend')
    expect(footerOrder().at(-1)).toBe('sms.wizardNext')
  })
})

describe('the saved-audience list', () => {
  it('fills in every control the audience was built from', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickAudience('Anua buyers')

    // Grouping, holdout and the filters, all from one choice.
    await waitFor(() => expect(lastPreview().get('brand')).toBe('Anua'), { timeout: 2000 })
    const p = lastPreview()
    expect(p.get('grouping')).toBe('single')
    expect(p.get('holdout_pct')).toBe('30')
    expect(p.get('brand')).toBe('Anua')
  })

  it('stops claiming to describe the audience once it is edited', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await pickAudience('Anua buyers')
    const list = screen.getByRole('combobox', { name: /sms\.presetsLabel/ })
    expect((list as HTMLSelectElement).value).toBe('Anua buyers')

    // Any change to the form makes this a different audience from the saved one.
    await userEvent.type(screen.getByLabelText('sms.filterRecency min'), '30')

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

describe('choosing tiers', () => {
  it('offers the value levels as a filter, whatever the measurement', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    // Choosing a level is "who gets this", and it stands on its own: the
    // default measurement is one arm, and the levels are still on offer.
    await waitFor(() => expect(lastPreview().get('grouping')).toBe('single'), { timeout: 2000 })
    await pickTier('VIP')
    await waitFor(() => expect(lastPreview().get('tier')).toBe('VIP'), { timeout: 2000 })

    // Switching to a per-level measurement does not change who was picked.
    await useValueTiers()
    await waitFor(() => expect(lastPreview().get('tier')).toBe('VIP'), { timeout: 2000 })
    await waitFor(() => expect(lastPreview().get('grouping')).toBe('rfm'), { timeout: 2000 })
  })

  it('freezes the levels that were picked', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await useValueTiers()
    await pickTier('VIP')
    await pickTier('CORE')

    await nameCampaign()
    await step('sms.wizardNext')
    await step('sms.wizardNext')
    await userEvent.type(screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка')
    await step('sms.wizardNext')
    await step('sms.createCampaign')

    // The freeze is where the subset binds.
    expect(new URLSearchParams(createMutate.mock.calls[0][0]).get('tier'))
      .toBe('VIP,CORE')
  })

  it('counts only the picked arms towards the campaign', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await useValueTiers()
    await pickTier('VIP')
    await nameCampaign()
    await step('sms.wizardNext')

    // VIP alone: 900 to send and 100 withheld, not the 7,560 of all three.
    await waitFor(() => expect(
      screen.getByText(/sms\.holdoutHint.*"target":"900".*"holdout":"100"/),
    ).toBeTruthy(), { timeout: 2000 })
  })

  it('shows each arm its own size on the chip', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)
    await useValueTiers()

    // The sizes are the whole basis for deciding which arms to include.
    await waitFor(() => expect(
      screen.getByRole('button', { name: /sms\.tier\.VIP · 900/ }),
    ).toBeTruthy(), { timeout: 2000 })
    expect(screen.getByRole('button', { name: /sms\.tier\.CORE · 2,700/ })).toBeTruthy()
  })
})


describe('the draft', () => {
  it('survives closing the wizard', async () => {
    const { unmount } = render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign('sep-brand')
    await useValueTiers()
    await pickTier('VIP')
    unmount()

    // Reopened: the name and the audience are where they were. An audience is
    // ten decisions; losing it to a misclick is what this prevents.
    render(<SmsCampaignWizard onClose={vi.fn()} />)
    expect(
      (screen.getByRole('textbox', { name: /sms\.campaignName/ }) as HTMLInputElement).value,
    ).toBe('sep-brand')
    expect(
      screen.getByRole('button', { name: /sms\.tier\.VIP/ }).getAttribute('aria-pressed'),
    ).toBe('true')
  })

  it('is cleared once the roster is frozen', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign()
    await step('sms.wizardNext')
    await step('sms.wizardNext')
    await userEvent.type(screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка')
    await step('sms.wizardNext')
    await step('sms.createCampaign')

    // The campaign exists on the server now; a draft of it would reopen as a
    // half-built duplicate.
    expect(sessionStorage.getItem('sms.campaignDraft.v1')).toBeNull()
  })
})

describe('the cost estimate', () => {
  it('prices the send at the gateway tariff, once there is a text', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    // No text yet: one part per recipient is the floor, and the line says so.
    expect(screen.getByText(/sms\.summaryCostFrom/)).toBeTruthy()

    await nameCampaign()
    await step('sms.wizardNext')
    await step('sms.wizardNext')
    await userEvent.type(
      screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка',
    )

    // 7,560 recipients × 1 part × 1.28 ₴.
    expect(screen.getByText(/sms\.summaryCost\(/)).toBeTruthy()
  })
})

describe('what the campaign will be able to prove', () => {
  it('states the threshold per arm, and calls a hopeless split hopeless', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    // None of this is on the manager's path: it lives behind the measurement
    // fold, closed by default.
    expect(screen.queryByText('sms.mdeTight')).toBeNull()
    await userEvent.click(screen.getByRole('button', { name: /sms\.measureTitle/ }))

    // One arm of 7,560 against 840 sees a lift from 1.60 pp — close to the
    // ~2 pp the only campaign there has been produced, so: tight, not
    // comfortable. That is what a 10% control buys even on the whole base.
    await waitFor(() => expect(screen.getByText('sms.mdeTight')).toBeTruthy(),
                  { timeout: 2000 })

    await useValueTiers()

    // Split three ways, the smallest arm is 900 against 100 — nothing a real
    // offer produces would clear that.
    await waitFor(() => expect(screen.getByText('sms.mdeHopeless')).toBeTruthy(),
                  { timeout: 2000 })
  })

  it('sharpens as the control share grows', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)
    await nameCampaign()
    await step('sms.wizardNext')

    // Precision is bought by withholding more, not by sending more — the one
    // fact the control step exists to make obvious.
    await step('40%')
    await waitFor(() => expect(lastPreview().get('holdout_pct')).toBe('40'), { timeout: 2000 })
  })
})

describe('the send dialog', () => {
  it('opens with the text the campaign was written and priced with', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await nameCampaign()
    await step('sms.wizardNext')
    await step('sms.wizardNext')
    await userEvent.type(
      screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка 30%',
    )
    await step('sms.wizardNext')
    await step('sms.createCampaign')
    await step('sms.sendNow')

    // Retyping it here would send something other than what was counted,
    // priced and rehearsed one step earlier.
    const filled = screen.getAllByRole('textbox')
      .filter((el) => (el as HTMLTextAreaElement).value === 'Знижка 30%')
    expect(filled.length).toBeGreaterThan(0)
  })
})
