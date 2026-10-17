import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState, type ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { TrafficSummaryCards } from './TrafficSummaryCards'
import { queryKeys } from '../hooks/useApi'
import { useQueryParams } from '../store/filterStore'
import type { TrafficAnalyticsResponse, TrafficSummary } from '../types/api'

// Illustrative numbers in production's proportions, not production's numbers.
// The paid halves add up to 216 new customers and the paid card says 214: two
// people bought through both on their first day, which is the whole reason
// new customers are counted as people rather than folded like orders.
const SUMMARY: TrafficSummary = {
  paid: { orders: 612, revenue: 1_356_330 },
  paid_confirmed: { orders: 430, revenue: 980_000 },
  paid_likely: { orders: 182, revenue: 376_330 },
  organic: { orders: 1_140, revenue: 2_180_400 },
  manager: { orders: 96, revenue: 210_000 },
  pixel_only: { orders: 290, revenue: 640_000 },
  unknown: { orders: 41, revenue: 95_000 },
}

function analytics(
  newCustomers: Record<keyof TrafficSummary, number>,
  total: number,
): TrafficAnalyticsResponse {
  return {
    period: { start: '2026-09-01', end: '2026-09-30' },
    totals: { orders: 2_179, revenue: 4_481_730 },
    summary: SUMMARY,
    by_platform: {},
    by_traffic_type: {},
    new_customers: { total, summary: newCustomers, by_platform: {}, by_traffic_type: {} },
  }
}

// The cards read their analytics themselves, so the story seeds the cache
// under the key the hook will ask for — the filter store's own query string —
// rather than passing data in through an API the component does not have.
function Seeded({ data, children }: { data: TrafficAnalyticsResponse; children: ReactNode }) {
  const params = useQueryParams()
  const [client] = useState(() => {
    const c = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    })
    c.setQueryData(queryKeys.trafficAnalytics(params), data)
    return c
  })
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}

const meta = {
  title: 'Traffic/TrafficSummaryCards',
  component: TrafficSummaryCards,
} satisfies Meta<typeof TrafficSummaryCards>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  render: () => (
    <Seeded
      data={analytics(
        {
          paid: 214, paid_confirmed: 150, paid_likely: 66, organic: 301,
          manager: 22, pixel_only: 118, unknown: 17,
        },
        665,
      )}
    >
      <TrafficSummaryCards />
    </Seeded>
  ),
}

/** A period where every order came from somebody who had bought before. */
export const NoNewCustomers: Story = {
  render: () => (
    <Seeded
      data={analytics(
        {
          paid: 0, paid_confirmed: 0, paid_likely: 0, organic: 0,
          manager: 0, pixel_only: 0, unknown: 0,
        },
        0,
      )}
    >
      <TrafficSummaryCards />
    </Seeded>
  ),
}
