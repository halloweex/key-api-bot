import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'

// src/lib/i18n.ts reads localStorage while being imported; see AdminUsersPage.test.tsx.
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
import { AdminPermissionsPage } from '../AdminPermissionsPage'
import { ToastProvider } from '../Toast'

// A click on one checkbox must send only the flag(s) it changed. The page used
// to send all three columns from its copy of the matrix, so a stale copy
// written back undid an earlier click on another column.

type Cell = { view: boolean; edit: boolean; delete: boolean }

let cell: Cell
let patches: Array<Record<string, string>>

function matrix() {
  return {
    permissions: { viewer: { reports: { ...cell } } },
    features: [{ key: 'reports', name: 'Reports', description: 'The reports tab' }],
    roles: [{ key: 'viewer', name: 'Viewer', description: 'Reads' }],
    tabs: [], presets: [], default_tabs: {},
  }
}

function json(body: unknown) {
  return new Response(JSON.stringify(body), {
    status: 200, headers: { 'Content-Type': 'application/json' },
  })
}

beforeEach(() => {
  cell = { view: true, edit: false, delete: false }
  patches = []
  vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(String(input), 'http://localhost')
    if (init?.method === 'PATCH') {
      const q = Object.fromEntries(url.searchParams)
      patches.push(q)
      // The server applies the change, as the real one does.
      if (q.can_view !== undefined) cell.view = q.can_view === 'true'
      if (q.can_edit !== undefined) cell.edit = q.can_edit === 'true'
      if (q.can_delete !== undefined) cell.delete = q.can_delete === 'true'
      return json({ success: true, role: q.role, feature: q.feature,
                    can_view: cell.view, can_edit: cell.edit, can_delete: cell.delete })
    }
    return json(matrix())
  }))
})

afterEach(() => {
  vi.unstubAllGlobals()
})

function renderPage() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  return render(
    <QueryClientProvider client={client}>
      <ToastProvider>
        <AdminPermissionsPage />
      </ToastProvider>
    </QueryClientProvider>,
  )
}

const box = (name: string) => screen.getByRole('checkbox', { name }) as HTMLInputElement

describe('AdminPermissionsPage — one click, one column', () => {
  it('sends only the flag that was toggled', async () => {
    renderPage()
    fireEvent.click(await screen.findByRole('checkbox', { name: 'Edit' }))

    await waitFor(() => expect(patches).toHaveLength(1))
    expect(patches[0]).toEqual({ role: 'viewer', feature: 'reports', can_edit: 'true' })
  })

  it('sends the flags a click implies, and only those', async () => {
    cell = { view: true, edit: true, delete: false }
    renderPage()
    // Taking View away takes Edit with it; Delete was already off.
    fireEvent.click(await screen.findByRole('checkbox', { name: 'View' }))

    await waitFor(() => expect(patches).toHaveLength(1))
    expect(patches[0]).toEqual({
      role: 'viewer', feature: 'reports', can_view: 'false', can_edit: 'false',
    })
  })

  it('computes the next click from the refreshed matrix, not a stale copy', async () => {
    renderPage()
    fireEvent.click(await screen.findByRole('checkbox', { name: 'Edit' }))
    // The cell stays disabled until the refetched matrix has replaced the old one.
    await waitFor(() => expect(box('Edit').checked).toBe(true))
    await waitFor(() => expect(box('Delete')).not.toBeDisabled())

    fireEvent.click(box('Delete'))
    await waitFor(() => expect(patches).toHaveLength(2))
    expect(patches[1]).toEqual({ role: 'viewer', feature: 'reports', can_delete: 'true' })
    await waitFor(() => expect(box('Delete').checked).toBe(true))
    expect(box('Edit').checked).toBe(true)
  })
})
