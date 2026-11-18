import type { Meta, StoryObj } from '@storybook/react-vite'
import { useLayoutEffect, useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { FilterBar } from './FilterBar'
import { authQueryKey } from '../hooks/useAuth'
import { queryKeys } from '../hooks/useApi'
import type { CurrentUserResponse, Permissions, UserRole } from '../types/api'

// The header's filters as a page shows them. `useAuth` decides which sales
// types are drawn (`internal` is an admin's alone), and the three lookups
// behind the selects are seeded empty: the layout is what these stories are
// for, not the options. Storybook's own path is no /traffic section, so every
// filter applies (`utils/pageFilters`).
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
    c.setQueryData(queryKeys.categories(), [])
    c.setQueryData(queryKeys.brands(), [])
    c.setQueryData(queryKeys.promocodes(), [])
    return c
  })
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}

/** Renders in one language and puts the reader's back when it unmounts. */
function InLanguage({ lng, children }: { lng: string; children: ReactNode }) {
  const { i18n } = useTranslation()
  useLayoutEffect(() => {
    const before = i18n.language
    void i18n.changeLanguage(lng)
    return () => { void i18n.changeLanguage(before) }
  }, [i18n, lng])
  return <>{children}</>
}

const meta = {
  title: 'Filters/FilterBar',
  component: FilterBar,
} satisfies Meta<typeof FilterBar>

export default meta
type Story = StoryObj<typeof meta>

/** A wide screen: one wrapping row, the sales types first. */
export const Desktop: Story = {
  render: () => (
    <Seeded role="viewer">
      <FilterBar />
    </Seeded>
  ),
}

/**
 * A phone, in Ukrainian, as an admin: the longest row of sales types there
 * is — six buttons, about 408 px, against the 168 px of half the two-column
 * grid. The sales types take a row of their own and scroll inside it; in a
 * half-row cell they ran over the source select beside them.
 */
export const Phone: Story = {
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  render: () => (
    <InLanguage lng="uk">
      <Seeded role="admin">
        <FilterBar />
      </Seeded>
    </InLanguage>
  ),
}
