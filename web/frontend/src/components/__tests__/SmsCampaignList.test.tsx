import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { SmsCampaignSummary } from '../../types/api'

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

const campaigns = vi.hoisted(() => ({ current: [] as Partial<SmsCampaignSummary>[] }))

vi.mock('../../hooks/useApi', () => ({
  useSmsCampaigns: () => ({ data: { campaigns: campaigns.current }, isLoading: false }),
  useMarkSmsCampaignSent: () => ({ mutate: vi.fn(), isPending: false }),
}))
vi.mock('../Toast', () => ({ useToast: () => ({ addToast: vi.fn() }) }))
vi.mock('../SmsSendDialog', () => ({ SmsSendDialog: () => null }))

import { SmsCampaignList } from '../SmsCampaignList'

function campaign(over: Partial<SmsCampaignSummary> = {}): Partial<SmsCampaignSummary> {
  return {
    campaign: 'aug-summer-sale', ltvBasis: 'margin', salesType: 'retail',
    holdoutPct: 10, promocode: null, exportedAt: '2026-08-26T18:16:00Z',
    sentAt: '2026-08-26T18:19:00Z', notes: null,
    members: 4671, target: 4199, holdout: 472, ...over,
  }
}

// ─── The list is how you get to a campaign's results ────────────────────────
//
// The results block sat directly below this table with its own campaign
// picker and no connection to it, so reading an older campaign meant noticing
// that a second dropdown existed and that it listed the same names.

describe('SmsCampaignList, as the way into a campaign', () => {
  it('offers to show the results of a campaign that was sent', async () => {
    const onSelect = vi.fn()
    campaigns.current = [campaign()]
    render(<SmsCampaignList onSelect={onSelect} />)

    await userEvent.click(screen.getByText('sms.viewResults'))
    expect(onSelect).toHaveBeenCalledWith('aug-summer-sale')
  })

  it('offers nothing of the sort for a campaign still waiting to go out', () => {
    campaigns.current = [campaign({ campaign: 'draft', sentAt: null })]
    render(<SmsCampaignList onSelect={vi.fn()} />)

    expect(screen.queryByText('sms.viewResults')).not.toBeInTheDocument()
    expect(screen.getByText('sms.send')).toBeInTheDocument()
  })

  it('marks which campaign is being read', () => {
    campaigns.current = [campaign(), campaign({ campaign: 'jul-promo' })]
    const { container } = render(
      <SmsCampaignList selected="jul-promo" onSelect={vi.fn()} />,
    )
    expect(container.querySelectorAll('tr.bg-purple-50\\/60')).toHaveLength(1)
  })

  it('still stands alone, with the name opening the campaign as before', async () => {
    campaigns.current = [campaign()]
    render(<SmsCampaignList />)

    expect(screen.queryByText('sms.viewResults')).not.toBeInTheDocument()
    await userEvent.click(screen.getByText('aug-summer-sale'))
    expect(screen.getByText('sms.detailsMessage')).toBeInTheDocument()
  })
})
