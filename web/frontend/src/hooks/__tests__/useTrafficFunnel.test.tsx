import { afterEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'

// Whose visits the funnel counts is the funnel's own filter: it must reach the
// funnel's request, and a change of it must ask again rather than show the
// answer for the other scope.

const calls: string[] = []

vi.mock('../../api/client', () => ({
  ApiError: class ApiError extends Error {
    status = 0
  },
  api: {
    getTrafficFunnel: (params: string) => {
      calls.push(params)
      return Promise.resolve({ country: new URLSearchParams(params).get('country') })
    },
  },
}))

// The funnel's page: the header sends it the period alone.
vi.mock('../useRouter', () => ({ useRouter: () => '/traffic' }))

import { useTrafficFunnel } from '../useApi'
import { useFilterStore } from '../../store/filterStore'

function wrapper() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  )
}

afterEach(() => {
  calls.length = 0
  useFilterStore.setState({ period: 'month', startDate: null, endDate: null, funnelCountry: 'ua' })
})

describe('the funnel request', () => {
  it('carries the country, Ukraine unless another is chosen', async () => {
    const { result } = renderHook(() => useTrafficFunnel(), { wrapper: wrapper() })
    await waitFor(() => expect(result.current.data).toEqual({ country: 'ua' }))
    expect(calls).toEqual(['period=month&country=ua'])
  })

  it('asks again when the country changes', async () => {
    const { result } = renderHook(() => useTrafficFunnel(), { wrapper: wrapper() })
    await waitFor(() => expect(result.current.data).toEqual({ country: 'ua' }))
    act(() => useFilterStore.getState().setFunnelCountry('all'))
    await waitFor(() => expect(result.current.data).toEqual({ country: 'all' }))
    expect(calls).toEqual(['period=month&country=ua', 'period=month&country=all'])
  })
})
