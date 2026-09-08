import { describe, it, expect, vi, beforeEach } from 'vitest'

// src/lib/i18n.ts reads localStorage while the module is being imported, and
// jsdom here exposes the object without its methods — SmsCampaignWizard's test
// hit the same wall.
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
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { AdminUsersPage } from '../AdminUsersPage'
import { api } from '../../api/client'
import type { AdminUser } from '../../types/api'

// ─── The tab toggle has to answer the click, not the network ────────────────
//
// It used to cost three sequential round trips before the tick appeared —
// PATCH, a refetch of the whole user list, then /api/me — with the chips
// disabled for all three, because a chip reads its state from the list query.
// Measured on production, the database was 2 ms of that; the rest was waiting.

const USER: AdminUser = {
  user_id: 4242,
  username: 'tester',
  first_name: 'Test',
  last_name: 'User',
  photo_url: null,
  role: 'viewer',
  status: 'approved',
  requested_at: null,
  reviewed_at: null,
  reviewed_by: null,
  last_activity: null,
  denial_count: 0,
  created_at: null,
  allowed_features: ['traffic'],
}

function renderPage() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  client.setQueryData(['adminUsers', null, null], { users: [USER], count: 1 })
  client.setQueryData(['adminPermissions'], {
    permissions: {}, features: [], roles: [],
    tabs: ['dashboard', 'products', 'traffic', 'inventory', 'reports',
           'marketing', 'margin', 'expenses', 'sms'],
    presets: [{ key: 'traffic_only', features: ['traffic'] }],
  })
  client.setQueryData(['currentUser'], {
    user: { id: 1, username: 'admin', first_name: 'A', last_name: '', photo_url: '', role: 'admin' },
    permissions: {},
  })
  return { client, ...render(
    <QueryClientProvider client={client}>
      <AdminUsersPage />
    </QueryClientProvider>,
  ) }
}

function openEditor() {
  // The summary in the Tabs column is the control that expands the editor.
  fireEvent.click(screen.getByText('access.tab.traffic', { selector: 'span' }))
}

describe('AdminUsersPage — tab toggles', () => {
  beforeEach(() => {
    vi.restoreAllMocks()
    vi.spyOn(api, 'getAdminUsers').mockResolvedValue({ users: [USER], count: 1 })
    vi.spyOn(api, 'getPermissionsMatrix').mockResolvedValue({
      permissions: {}, features: [], roles: [], tabs: [], presets: [],
    } as never)
    vi.spyOn(api, 'getAccessRequests').mockResolvedValue({ requests: [], count: 0 })
  })

  it('moves the chip before the request comes back', async () => {
    let resolve: (v: unknown) => void = () => {}
    const pending = new Promise((r) => { resolve = r })
    const patch = vi.spyOn(api, 'updateUserFeatures').mockReturnValue(pending as never)

    renderPage()
    openEditor()

    const chip = await screen.findByRole('button', { name: 'access.tab.reports' })
    fireEvent.click(chip)

    // The request has not answered yet, and the summary already says so.
    await waitFor(() => {
      expect(screen.getByText(/access\.tab\.reports/)).toBeInTheDocument()
    })
    expect(patch).toHaveBeenCalledWith(4242, ['traffic', 'reports'])

    resolve({ success: true, user_id: 4242, allowed_features: ['traffic', 'reports'] })
  })

  it('keeps the chips clickable while the request is in flight', async () => {
    const pending = new Promise(() => {})
    vi.spyOn(api, 'updateUserFeatures').mockReturnValue(pending as never)

    renderPage()
    openEditor()

    const chip = await screen.findByRole('button', { name: 'access.tab.reports' })
    fireEvent.click(chip)

    const margin = await screen.findByRole('button', { name: 'access.tab.margin' })
    expect(margin).not.toBeDisabled()
  })

  it('puts the old set back when the request fails', async () => {
    vi.spyOn(api, 'updateUserFeatures').mockRejectedValue(new Error('nope'))

    const { client } = renderPage()
    openEditor()

    const chip = await screen.findByRole('button', { name: 'access.tab.reports' })
    fireEvent.click(chip)

    await waitFor(() => {
      const cached = client.getQueryData(['adminUsers', null, null]) as { users: AdminUser[] }
      expect(cached.users[0].allowed_features).toEqual(['traffic'])
    })
  })
})
