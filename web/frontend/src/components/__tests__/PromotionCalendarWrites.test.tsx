import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { Promotion } from '../../types/api'
import { addDays, kyivToday } from '../../utils/smsDates'

// Writes that overlap, on the real hooks and the real API client: only
// `fetch` is stood in for, and each write's answer comes when the test says.
// One hook serves every row's «Скасувати», and TanStack calls the callbacks
// given to `mutate` for its newest call only: a second cancel sent before the
// first answered lost the first one's error toast and its form close, and
// its row stopped looking busy (review of #397, 2026-11-20).

vi.mock('../../hooks/useAuth', () => ({
  usePermission: (feature: string) => ({
    canView: feature === 'marketing',
    canEdit: feature === 'marketing',
    canDelete: false,
    isLoading: false,
  }),
}))

const toasts = vi.hoisted(() => ({ add: vi.fn() }))
vi.mock('../Toast', () => ({ useToast: () => ({ addToast: toasts.add }) }))

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

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
}

/** What the list answers now; a test moves it as the server would. */
let listed: Promotion[] = []
/** Every write sent, waiting for the test to answer it. */
let writes: { path: string; answer: (r: Response) => void }[] = []

beforeEach(() => {
  listed = [
    promotion({ id: 1, name: 'Весна' }),
    promotion({ id: 2, name: 'Літо', starts_on: addDays(today, 8), ends_on: addDays(today, 9) }),
  ]
  writes = []
  toasts.add.mockReset()
  vi.stubGlobal('fetch', vi.fn((url: string, init?: RequestInit) => {
    const path = new URL(url, 'https://dashboard.test').pathname
    if ((init?.method ?? 'GET') !== 'GET') {
      return new Promise<Response>((resolve) => writes.push({ path, answer: resolve }))
    }
    if (path === '/api/promotions') return Promise.resolve(json({ promotions: listed }))
    if (path === '/api/brands' || path === '/api/categories') return Promise.resolve(json([]))
    return Promise.resolve(json({ detail: 'Not Found' }, 404))
  }))
})

afterEach(() => {
  vi.unstubAllGlobals()
})

function renderCalendar() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  render(<QueryClientProvider client={client}><PromotionCalendar /></QueryClientProvider>)
}

const row = (name: string) => screen.getByText(name).closest('li') as HTMLElement
const button = (name: string, label: string) => within(row(name)).getByRole('button', { name: label })
const cancelled = (id: number) => listed.find((p) => p.id === id)!

function cancelOnServer(id: number): Promotion {
  listed = listed.map((p) => (p.id === id ? { ...p, version: p.version + 1, status: 'cancelled' } : p))
  return cancelled(id)
}

describe('two cancels on their way at once', () => {
  it('keeps both rows busy, and says how each one ended', async () => {
    renderCalendar()
    await screen.findByText('Весна')
    await userEvent.click(button('Весна', 'promo.cancel'))
    await userEvent.click(button('Літо', 'promo.cancel'))
    await waitFor(() => expect(writes.map((w) => w.path))
      .toEqual(['/api/promotions/1/cancel', '/api/promotions/2/cancel']))
    expect(button('Весна', 'promo.cancel')).toBeDisabled()
    expect(button('Літо', 'promo.cancel')).toBeDisabled()

    // The first is refused, then the second goes through.
    writes[0].answer(json({ detail: 'Service temporarily unavailable' }, 503))
    await waitFor(() => expect(toasts.add).toHaveBeenCalledWith(expect.objectContaining({
      type: 'error', title: 'promo.cancelFailed', message: 'Service temporarily unavailable',
    })))
    writes[1].answer(json(cancelOnServer(2)))
    await waitFor(() => expect(toasts.add).toHaveBeenCalledWith(expect.objectContaining({
      type: 'info', title: 'promo.cancelled',
    })))

    // The cancelled one leaves the list; the refused one may be tried again.
    await waitFor(() => expect(screen.queryByText('Літо')).not.toBeInTheDocument())
    expect(button('Весна', 'promo.cancel')).toBeEnabled()
    expect(toasts.add).toHaveBeenCalledTimes(2)
  })

  it('closes the form of the one cancelled first', async () => {
    renderCalendar()
    await screen.findByText('Весна')
    await userEvent.click(button('Весна', 'promo.edit'))
    expect(screen.getByLabelText('promo.field.name')).toHaveValue('Весна')

    await userEvent.click(button('Весна', 'promo.cancel'))
    await userEvent.click(button('Літо', 'promo.cancel'))
    await waitFor(() => expect(writes).toHaveLength(2))
    writes[0].answer(json(cancelOnServer(1)))
    writes[1].answer(json(cancelOnServer(2)))

    // Saving that form now would only be refused (409).
    await waitFor(() => expect(screen.queryByLabelText('promo.field.name')).not.toBeInTheDocument())
  })
})

describe('a save', () => {
  it('closes only the form it was sent from', async () => {
    renderCalendar()
    await screen.findByText('Весна')
    await userEvent.click(button('Весна', 'promo.edit'))
    await userEvent.click(screen.getByRole('button', { name: 'promo.save' }))
    await waitFor(() => expect(writes.map((w) => w.path)).toEqual(['/api/promotions/1']))

    // While it is on its way the person closes it and opens another one.
    await userEvent.click(screen.getByRole('button', { name: 'promo.close' }))
    await userEvent.click(button('Літо', 'promo.edit'))
    writes[0].answer(json({ ...listed[0], version: 2 }))

    await waitFor(() => expect(toasts.add).toHaveBeenCalledWith(expect.objectContaining({
      type: 'success', title: 'promo.saved', message: 'Весна',
    })))
    expect(screen.getByLabelText('promo.field.name')).toHaveValue('Літо')
  })

  it('closes its own form when it goes through, and keeps it open when refused', async () => {
    renderCalendar()
    await screen.findByText('Весна')
    await userEvent.click(button('Весна', 'promo.edit'))
    await userEvent.click(screen.getByRole('button', { name: 'promo.save' }))
    await waitFor(() => expect(writes).toHaveLength(1))
    writes[0].answer(json({ detail: 'name: Is required' }, 400))
    await waitFor(() => expect(toasts.add).toHaveBeenCalledWith(expect.objectContaining({
      type: 'error', title: 'promo.saveFailed', message: 'name: Is required',
    })))
    expect(screen.getByLabelText('promo.field.name')).toHaveValue('Весна')

    await userEvent.click(screen.getByRole('button', { name: 'promo.save' }))
    await waitFor(() => expect(writes).toHaveLength(2))
    writes[1].answer(json({ ...listed[0], version: 2 }))
    await waitFor(() => expect(screen.queryByLabelText('promo.field.name')).not.toBeInTheDocument())
  })
})
