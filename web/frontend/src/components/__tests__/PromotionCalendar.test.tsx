import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import type { Promotion } from '../../types/api'
import { addDays, kyivToday } from '../../utils/smsDates'

// Who may do what on the calendar. The lists are every reader's; adding,
// changing, cancelling and restoring are `marketing` edit's; the SMS
// campaign a promotion came from is offered only to a reader of SMS
// campaigns. And /marketing#promotions opens the form for whoever may add.

const perms = vi.hoisted(() => ({
  current: {} as Record<string, { view: boolean; edit: boolean }>,
}))
vi.mock('../../hooks/useAuth', () => ({
  usePermission: (feature: string) => ({
    canView: perms.current[feature]?.view ?? false,
    canEdit: perms.current[feature]?.edit ?? false,
    canDelete: false,
    isLoading: false,
  }),
}))

const store = vi.hoisted(() => ({
  promotions: [] as Promotion[],
  save: vi.fn(),
  cancel: vi.fn(),
  restore: vi.fn(),
  smsEnabled: [] as boolean[],
}))
vi.mock('../../hooks/useApi', () => ({
  usePromotions: () => ({
    data: { promotions: store.promotions }, isLoading: false, error: null, refetch: vi.fn(),
  }),
  usePromotionVersions: () => ({ data: undefined, isLoading: false, error: null, refetch: vi.fn() }),
  useSavePromotion: () => ({ mutate: store.save, isPending: false }),
  useCancelPromotion: () => ({ mutate: store.cancel, isPending: false }),
  useRestorePromotion: () => ({ mutate: store.restore, isPending: false }),
  useSmsCampaigns: (enabled: boolean) => {
    store.smsEnabled.push(enabled)
    return { data: enabled ? { campaigns: [] } : undefined }
  },
  useBrands: () => ({ data: [{ name: 'COSRX' }] }),
  useCategories: () => ({ data: [] }),
}))
vi.mock('../Toast', () => ({ useToast: () => ({ addToast: vi.fn() }) }))

import { PromotionCalendar } from '../PromotionCalendar'

const today = kyivToday()

function promotion(over: Partial<Promotion>): Promotion {
  return {
    id: 1, version: 1, name: 'Весна', starts_on: addDays(today, 5), ends_on: addDays(today, 7),
    start_time: null, kind: 'other', discount_pct: 20, scope: 'store', scope_items: [],
    channels: ['sms'], sms_campaign: null, note: null, status: 'planned',
    created_at: '2026-11-19T10:12:03+02:00', updated_at: '2026-11-19T10:12:03+02:00',
    source: 'form', ...over,
  }
}

const VIEWER = { marketing: { view: true, edit: false } }
const EDITOR = { marketing: { view: true, edit: true } }
const EDITOR_WITH_SMS = { ...EDITOR, sms: { view: true, edit: false } }

beforeEach(() => {
  store.promotions = [
    promotion({ id: 1, name: 'Весна' }),
    promotion({ id: 2, name: 'Літо', starts_on: addDays(today, -20), ends_on: addDays(today, -18) }),
    promotion({ id: 3, name: 'Скасована', status: 'cancelled', starts_on: addDays(today, 10), ends_on: addDays(today, 11) }),
  ]
  store.smsEnabled = []
  store.save.mockReset()
  store.cancel.mockReset()
  store.restore.mockReset()
})

afterEach(() => {
  window.history.replaceState(null, '', '/')
  // jsdom has none; a test that wants to see the scroll lends one.
  delete (Element.prototype as Partial<Element>).scrollIntoView
})

describe('the calendar for somebody who may only read it', () => {
  it('shows the upcoming and past lists and nothing that writes', () => {
    perms.current = VIEWER
    render(<PromotionCalendar />)
    expect(screen.getByText('Весна')).toBeInTheDocument()
    expect(screen.getByText('Літо')).toBeInTheDocument()
    for (const name of ['promo.add', 'promo.edit', 'promo.cancel', 'promo.restore']) {
      expect(screen.queryByRole('button', { name })).not.toBeInTheDocument()
    }
  })

  it('keeps the cancelled ones behind the toggle', async () => {
    perms.current = VIEWER
    render(<PromotionCalendar />)
    expect(screen.queryByText('Скасована')).not.toBeInTheDocument()
    await userEvent.click(screen.getByLabelText('promo.showCancelled{1}'))
    expect(screen.getByText('Скасована')).toBeInTheDocument()
    // Still nothing to restore it with.
    expect(screen.queryByRole('button', { name: 'promo.restore' })).not.toBeInTheDocument()
  })
})

describe('the calendar for somebody with marketing edit', () => {
  it('adds, changes and cancels', async () => {
    perms.current = EDITOR
    render(<PromotionCalendar />)
    expect(screen.queryByLabelText('promo.field.name')).not.toBeInTheDocument()

    await userEvent.click(screen.getByRole('button', { name: 'promo.add' }))
    expect(screen.getByLabelText('promo.field.name')).toHaveValue('')

    const spring = screen.getByText('Весна').closest('li') as HTMLElement
    await userEvent.click(within(spring).getByRole('button', { name: 'promo.edit' }))
    expect(screen.getByLabelText('promo.field.name')).toHaveValue('Весна')

    await userEvent.click(within(spring).getByRole('button', { name: 'promo.cancel' }))
    expect(store.cancel).toHaveBeenCalledWith(1)
  })

  it('restores a cancelled one, and does not offer to change it', async () => {
    perms.current = EDITOR
    render(<PromotionCalendar />)
    await userEvent.click(screen.getByLabelText('promo.showCancelled{1}'))
    const cancelled = screen.getByText('Скасована').closest('li') as HTMLElement
    expect(within(cancelled).queryByRole('button', { name: 'promo.edit' })).not.toBeInTheDocument()
    await userEvent.click(within(cancelled).getByRole('button', { name: 'promo.restore' }))
    expect(store.restore).toHaveBeenCalledWith(3)
  })

  it('saves a changed promotion as its next version, under its id', async () => {
    perms.current = EDITOR
    render(<PromotionCalendar />)
    const spring = screen.getByText('Весна').closest('li') as HTMLElement
    await userEvent.click(within(spring).getByRole('button', { name: 'promo.edit' }))
    await userEvent.click(screen.getByRole('button', { name: 'promo.save' }))
    expect(store.save).toHaveBeenCalledWith(
      expect.objectContaining({ id: 1, body: expect.objectContaining({ name: 'Весна' }) }),
    )
  })

  it('offers an SMS campaign only to a reader of SMS campaigns, and asks for them only then', async () => {
    perms.current = EDITOR
    const { unmount } = render(<PromotionCalendar />)
    await userEvent.click(screen.getByRole('button', { name: 'promo.add' }))
    expect(screen.queryByLabelText('promo.field.fromSms')).not.toBeInTheDocument()
    expect(store.smsEnabled.every((enabled) => !enabled)).toBe(true)
    unmount()

    perms.current = EDITOR_WITH_SMS
    render(<PromotionCalendar />)
    expect(store.smsEnabled.at(-1)).toBe(false) // not while the form is closed
    await userEvent.click(screen.getByRole('button', { name: 'promo.add' }))
    expect(screen.getByLabelText('promo.field.fromSms')).toBeInTheDocument()
    expect(store.smsEnabled.at(-1)).toBe(true)
  })
})

describe('/marketing#promotions', () => {
  it('opens the form for somebody who may add, and scrolls the section into view', async () => {
    const scrolled = vi.fn()
    Element.prototype.scrollIntoView = scrolled
    window.history.replaceState(null, '', '/marketing#promotions')
    perms.current = EDITOR
    const { container } = render(<PromotionCalendar />)
    expect(screen.getByLabelText('promo.field.name')).toBeInTheDocument()
    expect(container.querySelector('section#promotions')).not.toBeNull()
    await waitFor(() => expect(scrolled).toHaveBeenCalled())
    expect(scrolled.mock.contexts[0]).toBe(container.querySelector('section#promotions'))
  })

  it('opens it with Telegram’s launch parameters after the anchor', () => {
    window.history.replaceState(null, '', '/marketing#promotions?tgWebAppData=x&tgWebAppVersion=8.0')
    perms.current = EDITOR
    render(<PromotionCalendar />)
    expect(screen.getByLabelText('promo.field.name')).toBeInTheDocument()
  })

  it('only scrolls for somebody who may not add', async () => {
    const scrolled = vi.fn()
    Element.prototype.scrollIntoView = scrolled
    window.history.replaceState(null, '', '/marketing#promotions')
    perms.current = VIEWER
    render(<PromotionCalendar />)
    expect(screen.queryByLabelText('promo.field.name')).not.toBeInTheDocument()
    await waitFor(() => expect(scrolled).toHaveBeenCalled())
  })

  it('leaves the form closed without the anchor', () => {
    window.history.replaceState(null, '', '/marketing')
    perms.current = EDITOR
    render(<PromotionCalendar />)
    expect(screen.queryByLabelText('promo.field.name')).not.toBeInTheDocument()
  })
})
