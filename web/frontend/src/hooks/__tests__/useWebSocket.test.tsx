import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, renderHook } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { useWebSocket } from '../useWebSocket'

// A sync that brought orders asks again for what they move. The month badge
// (`/api/revenue/forecast`: its range, the Black Friday line, the planned
// promotions) stands beside the trend's live point, so it is asked with it:
// past midnight or a month's end the point was the new day's while the badge
// still said the old one's (review of #397, 2026-11-20).

class FakeSocket {
  static readonly CONNECTING = 0
  static readonly OPEN = 1
  static readonly CLOSING = 2
  static readonly CLOSED = 3
  static last: FakeSocket | null = null

  readonly url: string
  readyState = FakeSocket.CONNECTING
  onopen: (() => void) | null = null
  onmessage: ((event: MessageEvent) => void) | null = null
  onerror: ((event: Event) => void) | null = null
  onclose: ((event: CloseEvent) => void) | null = null

  constructor(url: string) {
    this.url = url
    FakeSocket.last = this
  }

  send() {}

  close() {
    this.readyState = FakeSocket.CLOSED
  }

  /** What the server broadcasts to the dashboard room. */
  receive(event: string, data: Record<string, unknown> = {}) {
    this.onmessage?.({ data: JSON.stringify({ event, data, timestamp: '2026-12-01T00:04:00Z' }) } as MessageEvent)
  }
}

let client: QueryClient

beforeEach(() => {
  vi.stubGlobal('WebSocket', FakeSocket)
  vi.spyOn(console, 'log').mockImplementation(() => {})
  client = new QueryClient()
  for (const key of [
    ['revenueForecast', 'retail'],
    ['revenueForecast', 'b2b'],
    ['revenueTrend', 'period=month'],
    ['promotions'],
  ]) {
    client.setQueryData(key, {})
  }
})

afterEach(() => {
  FakeSocket.last = null
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

function connect() {
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  )
  renderHook(() => useWebSocket(), { wrapper })
  const socket = FakeSocket.last
  if (!socket) throw new Error('the hook opened no socket')
  return socket
}

const invalidated = (key: string[]) => client.getQueryState(key)?.isInvalidated

describe('orders synced', () => {
  it('asks the month badge again for every sales type, beside the live point', () => {
    const socket = connect()
    expect(invalidated(['revenueForecast', 'retail'])).toBe(false)

    act(() => socket.receive('orders_synced', { count: 3 }))

    expect(invalidated(['revenueTrend', 'period=month'])).toBe(true)
    expect(invalidated(['revenueForecast', 'retail'])).toBe(true)
    expect(invalidated(['revenueForecast', 'b2b'])).toBe(true)
    // Orders move no promotion.
    expect(invalidated(['promotions'])).toBe(false)
  })

  it('is the one event that does', () => {
    const socket = connect()
    act(() => {
      socket.receive('products_synced', { count: 3 })
      socket.receive('inventory_updated')
      socket.receive('goal_progress')
    })
    expect(invalidated(['revenueForecast', 'retail'])).toBe(false)
  })
})
