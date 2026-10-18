import { describe, it, expect, vi, beforeEach } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'

// The preview is capped at 60 a minute per client. Nine template counts and a
// group's diagnosis must therefore ask in turn, never in a burst, and a count
// once had must not be asked again on the next render.

const pending = new Map<string, (value: unknown) => void>()
const calls: string[] = []

vi.mock('../../api/client', () => ({
  api: {
    getSmsSegments: (params: string) => {
      calls.push(params)
      return new Promise((resolve, reject) => {
        pending.set(params, (value) => (value instanceof Error ? reject(value) : resolve(value)))
      })
    },
  },
}))

import { useSmsAudienceCounts } from '../useApi'

const answer = (customers: number) => ({ totals: { customers, target: customers, holdout: 0 } })

function wrapper() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  )
}

beforeEach(() => {
  pending.clear()
  calls.length = 0
})

const items = [
  { key: 'a', params: 'q=a' },
  { key: 'none', params: null },
  { key: 'b', params: 'q=b' },
  { key: 'c', params: 'q=c' },
]

describe('counts of other audiences', () => {
  it('asks one at a time, each after the one before it has settled', async () => {
    const { result } = renderHook(() => useSmsAudienceCounts(items), { wrapper: wrapper() })

    await waitFor(() => expect(calls).toEqual(['q=a']))
    expect(result.current.a).toEqual({ status: 'loading' })
    expect(result.current.b).toEqual({ status: 'loading' })
    expect(result.current.none).toBeUndefined()

    await act(async () => pending.get('q=a')!(answer(1185)))
    await waitFor(() => expect(calls).toEqual(['q=a', 'q=b']))
    expect(result.current.a).toMatchObject({ status: 'done', data: answer(1185) })

    // A failure is settled too: the queue moves on, and the count shows nothing.
    await act(async () => pending.get('q=b')!(new Error('429')))
    await waitFor(() => expect(calls).toEqual(['q=a', 'q=b', 'q=c']))
    expect(result.current.b).toEqual({ status: 'error' })
  })

  it('asks a question two items share once, and answers both', async () => {
    const { result } = renderHook(() => useSmsAudienceCounts([
      { key: 'template', params: 'q=a' }, { key: 'none-1', params: null },
      { key: 'none-2', params: null }, { key: 'probe', params: 'q=a' },
    ]), { wrapper: wrapper() })

    await waitFor(() => expect(calls).toEqual(['q=a']))
    await act(async () => pending.get('q=a')!(answer(42)))
    await waitFor(() => expect(result.current.probe).toMatchObject({ status: 'done' }))
    expect(result.current.template).toMatchObject({ status: 'done', data: answer(42) })
    expect(calls).toEqual(['q=a'])
  })

  it('works through a long queue of instant answers without stalling', async () => {
    // Nine templates behind nine unasked ones: the shape that stalled in a
    // browser while every unasked item still shared one placeholder key.
    const many = Array.from({ length: 9 }, (_, i) => [
      { key: `blank${i}`, params: null }, { key: `t${i}`, params: `q=${i}` },
    ]).flat()
    const { result } = renderHook(() => useSmsAudienceCounts(many), { wrapper: wrapper() })
    for (let i = 0; i < 9; i++) {
      await waitFor(() => expect(pending.has(`q=${i}`)).toBe(true))
      await act(async () => pending.get(`q=${i}`)!(answer(i)))
    }
    await waitFor(() => expect(result.current.t8).toMatchObject({ status: 'done' }))
    expect(calls).toEqual(Array.from({ length: 9 }, (_, i) => `q=${i}`))
  })

  it('asks nothing until it is enabled', async () => {
    renderHook(() => useSmsAudienceCounts(items, false), { wrapper: wrapper() })
    await new Promise((r) => setTimeout(r, 20))
    expect(calls).toEqual([])
  })

  it('does not ask again for a count it already has', async () => {
    const make = wrapper()
    const first = renderHook(() => useSmsAudienceCounts([items[0]]), { wrapper: make })
    await waitFor(() => expect(calls).toEqual(['q=a']))
    await act(async () => pending.get('q=a')!(answer(7)))
    first.unmount()

    const again = renderHook(() => useSmsAudienceCounts([items[0]]), { wrapper: make })
    expect(again.result.current.a).toMatchObject({ status: 'done' })
    await new Promise((r) => setTimeout(r, 20))
    expect(calls).toEqual(['q=a'])
  })
})
