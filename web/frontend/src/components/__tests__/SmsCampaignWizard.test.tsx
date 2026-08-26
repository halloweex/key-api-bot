import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
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
  segments: [
    { tier: 'ALL', total: 8400, target: 7560, holdout: 840, totalLtv: 0,
      avgLtv: 3900, totalRevenue: 0, totalMargin: 0, marginPct: 55,
      avgOrders: 2.3, avgRecencyDays: 98 },
  ],
  totals: { customers: 8400, target: 7560, holdout: 840 },
  truncated: false,
}

const segmentParams: string[] = []
const createMutate = vi.fn()
const savePresetMutate = vi.fn()

vi.mock('../../hooks/useApi', () => ({
  useSmsSegments: (params: string) => {
    segmentParams.push(params)
    return { data, isLoading: false, error: null, refetch: vi.fn() }
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
  useSmsChannels: () => ({ data: { sms: true, viber: false } }),
  useSendTestSms: () => ({ mutate: vi.fn(), isPending: false }),
  useSendSmsCampaign: () => ({ mutate: vi.fn(), isPending: false }),
}))

vi.mock('../Toast', () => ({ useToast: () => ({ addToast: vi.fn() }) }))

beforeEach(() => {
  segmentParams.length = 0
  createMutate.mockClear()
  savePresetMutate.mockClear()
})

/** The parameters of the most recent preview query. */
function lastPreview(): URLSearchParams {
  return new URLSearchParams(segmentParams[segmentParams.length - 1])
}

async function step(name: string) {
  await userEvent.click(screen.getByRole('button', { name }))
}

describe('SmsCampaignWizard', () => {
  it('previews the plain cohort before anything is touched', () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    expect(lastPreview().get('grouping')).toBe('rfm')
    expect(lastPreview().has('brand')).toBe(false)
  })

  it('applies a saved audience to the live preview', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await step('Anua buyers')

    const p = lastPreview()
    expect(p.get('grouping')).toBe('single')
    expect(p.get('brand')).toBe('Anua')
    expect(p.get('holdout_pct')).toBe('30')
  })

  it('saves the audience currently on screen under a name', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await step('Anua buyers')
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

    await step('Anua buyers')
    await step('sms.wizardNext')          // → control
    await step('30%')
    await step('sms.wizardNext')          // → message
    await userEvent.type(
      screen.getByRole('textbox', { name: /sms\.campaignName/ }), 'sep-brand',
    )
    await userEvent.type(screen.getByRole('textbox', { name: /sms\.messageText/ }), 'Знижка')
    await step('sms.wizardNext')          // → launch
    await step('sms.createCampaign')

    const sent = new URLSearchParams(createMutate.mock.calls[0][0])
    expect(sent.get('campaign')).toBe('sep-brand')
    expect(sent.get('brand')).toBe('Anua')
    expect(sent.get('grouping')).toBe('single')
    expect(sent.get('holdout_pct')).toBe('30')
    // The preview and the freeze must not diverge: same audience, same query.
    const preview = lastPreview()
    for (const key of ['grouping', 'brand', 'holdout_pct', 'ltv_basis']) {
      expect(sent.get(key)).toBe(preview.get(key))
    }
  })

  it('will not freeze a campaign without a name', async () => {
    render(<SmsCampaignWizard onClose={vi.fn()} />)

    await step('sms.wizardNext')
    await step('sms.wizardNext')
    // The step guard itself refuses to advance without a valid name.
    const next = screen.getByRole('button', { name: 'sms.wizardNext' })
    expect(next.hasAttribute('disabled')).toBe(true)
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
