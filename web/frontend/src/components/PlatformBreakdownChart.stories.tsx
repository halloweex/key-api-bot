import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState, type ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { PlatformBreakdownChart } from './PlatformBreakdownChart'
import { queryKeys } from '../hooks/useApi'
import { useQueryParams } from '../store/filterStore'
import type { TrafficAnalyticsResponse, TrafficSummary } from '../types/api'

// Illustrative numbers in production's proportions, not production's numbers.
const BY_PLATFORM: TrafficAnalyticsResponse['by_platform'] = {
  instagram: { orders: 1_020, revenue: 1_980_000 },
  facebook: { orders: 430, revenue: 980_000 },
  unattributed: { orders: 290, revenue: 640_000 },
  google_ads: { orders: 120, revenue: 260_000 },
  tiktok: { orders: 62, revenue: 116_330 },
  telegram: { orders: 120, revenue: 200_400 },
  google_organic: { orders: 41, revenue: 95_000 },
}

const NEW_BY_PLATFORM: Record<string, number> = {
  instagram: 260, facebook: 150, unattributed: 118, google_ads: 44,
  tiktok: 22, telegram: 41, google_organic: 17,
}

const EMPTY_SUMMARY = Object.fromEntries(
  ['paid', 'paid_confirmed', 'paid_likely', 'organic', 'manager', 'pixel_only', 'unknown']
    .map((k) => [k, 0]),
) as Record<keyof TrafficSummary, number>

const ANALYTICS = {
  period: { start: '2026-09-01', end: '2026-09-30' },
  totals: { orders: 2_083, revenue: 4_271_730 },
  summary: {} as TrafficSummary,
  by_platform: BY_PLATFORM,
  by_traffic_type: {},
  new_customers: {
    total: 645, summary: EMPTY_SUMMARY, by_platform: NEW_BY_PLATFORM, by_traffic_type: {},
  },
} satisfies TrafficAnalyticsResponse

// The chart reads its analytics itself, so the story seeds the cache under the
// key the hook will ask for — the filter store's own query string.
function Seeded({ children }: { children: ReactNode }) {
  const params = useQueryParams()
  const [client] = useState(() => {
    const c = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    })
    c.setQueryData(queryKeys.trafficAnalytics(params), ANALYTICS)
    return c
  })
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}

const meta = {
  title: 'Traffic/PlatformBreakdownChart',
  component: PlatformBreakdownChart,
} satisfies Meta<typeof PlatformBreakdownChart>

export default meta
type Story = StoryObj<typeof meta>

/** Hover a slice or a bar: the tooltip carries the platform's new customers. */
export const Default: Story = {
  render: () => (
    <Seeded>
      <PlatformBreakdownChart />
    </Seeded>
  ),
}
