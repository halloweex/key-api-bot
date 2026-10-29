import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState, type ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { PixelOnlyBreakdown } from './PixelOnlyBreakdown'
import { queryKeys } from '../hooks/useApi'
import { useQueryParams } from '../store/filterStore'
import type { PixelOnlyFirstChannel, TrafficPixelOnlyResponse } from '../types/api'

// Illustrative numbers in production's proportions, not production's numbers:
// about four in ten pixel-only buyers are new, and most returning ones first
// came through Instagram Direct or the profile link.

function row(
  group: PixelOnlyFirstChannel['group'], channel: string,
  buyers: number, orders: number, revenue: number, newBuyers = 0,
): PixelOnlyFirstChannel {
  return { group, channel, buyers, orders, revenue, new_buyers: newBuyers }
}

const DATA: TrafficPixelOnlyResponse = {
  period: { start: '2026-09-01', end: '2026-09-30' },
  totals: { orders: 330, revenue: 780_000, buyers: 290 },
  new: { orders: 125, revenue: 260_000, buyers: 118 },
  returning: { orders: 205, revenue: 520_000, buyers: 174 },
  first_channels: [
    row('pixel_only', 'pixel_only', 140, 150, 330_000, 118),
    row('organic', 'instagram_direct', 45, 55, 150_000),
    row('unknown', 'unknown', 38, 42, 95_000),
    row('organic', 'instagram_site', 22, 25, 70_000),
    row('paid', 'facebook', 14, 16, 45_000),
    row('organic', 'email_flows', 9, 10, 25_000),
    row('paid', 'google_ads', 7, 8, 21_000),
    row('organic', 'google_organic', 5, 5, 9_000),
    row('unknown', 'opencart', 4, 5, 15_000),
    row('manager', 'manager', 2, 2, 5_000),
  ],
}

function Seeded({ data, children }: { data: TrafficPixelOnlyResponse; children: ReactNode }) {
  const params = useQueryParams()
  const [client] = useState(() => {
    const c = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } })
    c.setQueryData(queryKeys.trafficPixelOnly(params), data)
    return c
  })
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}

const meta = {
  title: 'Traffic/PixelOnlyBreakdown',
  component: PixelOnlyBreakdown,
} satisfies Meta<typeof PixelOnlyBreakdown>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  render: () => (
    <Seeded data={DATA}>
      <PixelOnlyBreakdown />
    </Seeded>
  ),
}

export const Empty: Story = {
  render: () => (
    <Seeded data={{
      ...DATA,
      totals: { orders: 0, revenue: 0, buyers: 0 },
      new: { orders: 0, revenue: 0, buyers: 0 },
      returning: { orders: 0, revenue: 0, buyers: 0 },
      first_channels: [],
    }}>
      <PixelOnlyBreakdown />
    </Seeded>
  ),
}
