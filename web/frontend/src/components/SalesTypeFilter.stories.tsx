import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState, type ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SalesTypeFilter } from './SalesTypeFilter'
import { authQueryKey } from '../hooks/useAuth'
import type { CurrentUserResponse, Permissions, UserRole } from '../types/api'

// The filter asks `useAuth` who is looking, so the story seeds the cache under
// the key that hook reads. Only the role matters here: `internal` is the one
// admin-only button, and the offline store (source 7, its own sales type since
// the owner's decision of 2026-11-15) is shown to every viewer.
function Seeded({ role, children }: { role: UserRole; children: ReactNode }) {
  const [client] = useState(() => {
    const c = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    })
    const me: CurrentUserResponse = {
      user: { id: 1, username: 'viewer', first_name: 'A', last_name: 'Viewer', photo_url: '', role },
      permissions: {} as Permissions,
    }
    c.setQueryData(authQueryKey, me)
    return c
  })
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}

const meta = {
  title: 'Filters/SalesTypeFilter',
  component: SalesTypeFilter,
} satisfies Meta<typeof SalesTypeFilter>

export default meta
type Story = StoryObj<typeof meta>

/** Retail, the offline store, wholesale, the exhibition and all sales. */
export const Viewer: Story = {
  render: () => (
    <Seeded role="viewer">
      <SalesTypeFilter />
    </Seeded>
  ),
}

/** An admin also sees `internal`. */
export const Admin: Story = {
  render: () => (
    <Seeded role="admin">
      <SalesTypeFilter />
    </Seeded>
  ),
}

/** At a phone's width, alone. The header gives the sales types a row of
 *  their own there, and the row scrolls when the buttons do not fit it —
 *  `Filters/FilterBar`'s `Phone` shows them in the header's grid. */
export const Phone: Story = {
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  render: () => (
    <Seeded role="viewer">
      <SalesTypeFilter />
    </Seeded>
  ),
}
