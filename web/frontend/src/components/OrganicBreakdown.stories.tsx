import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState, type ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { OrganicBreakdown } from './OrganicBreakdown'
import { queryKeys } from '../hooks/useApi'
import { useQueryParams } from '../store/filterStore'
import type {
  OrganicChannel,
  TrafficAnalyticsResponse,
  TrafficOrganicResponse,
} from '../types/api'

// Illustrative numbers in production's proportions for one month of retail,
// not production's numbers: Direct about two thirds of organic, the profile
// link a fifth, email split between automated letters and newsletters.

// One GA4-shaped row: source / medium, campaign, content, orders, revenue.
type Row = [string, string, string, string, number, number]

const UNTAGGED = (orders: number, revenue: number): Row => ['', '', '', '', orders, revenue]

function channel(
  key: OrganicChannel['channel'],
  orders: number,
  revenue: number,
  newCustomers: number,
  previous: [number, number],
  details: Row[] = [],
  detailsTotal = details.length,
): OrganicChannel {
  return {
    channel: key,
    orders,
    revenue,
    new_customers: newCustomers,
    previous: { orders: previous[0], revenue: previous[1] },
    details: details.map(([source, medium, campaign, content, o, r]) => (
      { source, medium, campaign, content, orders: o, revenue: r })),
    details_total: detailsTotal,
  }
}

const CHANNELS: OrganicChannel[] = [
  channel('instagram_direct', 510, 1_520_000, 160, [470, 1_390_000],
    [UNTAGGED(510, 1_520_000)]),
  channel('instagram_site', 195, 520_000, 82, [210, 560_000],
    [['ig', 'social', '', 'link_in_bio', 192, 512_000],
     ['ig', 'social', '23_06_catalog_clients', 'link_in_bio', 3, 8_000]]),
  channel('telegram', 35, 115_000, 11, [31, 98_000], [UNTAGGED(35, 115_000)]),
  channel('email_flows', 44, 112_000, 26, [38, 96_000],
    [['klaviyo', 'email', 'Welcome Email 1a: Welcome & Gift Reveal (Vt8RBn)', '', 18, 41_000],
     ['klaviyo', 'email', 'Cart Abandon Email 1: Quick Cart Reminder (TeZB9D)', '', 7, 21_000],
     ['rivo', 'email', 'rivo-loyalty-points-earned-email', '', 5, 12_000],
     ['klaviyo', '', '()', '', 3, 6_000]], 14),
  channel('google_organic', 33, 55_000, 22, [29, 48_000],
    [['google', 'product_sync', 'sag_organic', 'sag_organic', 33, 55_000]]),
  channel('email_campaigns', 14, 38_000, 3, [9, 22_000],
    [['klaviyo', 'klaviyo', '[10/06/26] Campaign #3: [PR] Тиждень Wellage', '', 6, 17_000],
     ['klaviyo', 'klaviyo', '[09/12/26] Campaign #6: [SP] 5 помилок при переході на осінній догляд', '', 4, 11_000]], 5),
  channel('instagram_stories', 9, 18_000, 6, [0, 0],
    [['instagram', 'stories_tanya_pads', 'tanya10', '', 6, 12_000],
     ['instagram', 'stories_anna15', '', '', 3, 6_000]]),
  channel('ai', 3, 6_500, 2, [2, 4_100], [['chatgpt.com', '', '', '', 3, 6_500]]),
]

function organic(channels: OrganicChannel[]): TrafficOrganicResponse {
  const sum = (pick: (c: OrganicChannel) => number) => channels.reduce((s, c) => s + pick(c), 0)
  return {
    period: { start: '2026-09-01', end: '2026-09-30' },
    previous_period: { start: '2026-08-02', end: '2026-08-31' },
    totals: {
      orders: sum((c) => c.orders),
      revenue: sum((c) => c.revenue),
      new_customers: 305,
    },
    previous_totals: {
      orders: sum((c) => c.previous.orders),
      revenue: sum((c) => c.previous.revenue),
    },
    channels,
  }
}

const ZERO = { orders: 0, revenue: 0 }

const ANALYTICS: TrafficAnalyticsResponse = {
  period: { start: '2026-09-01', end: '2026-09-30' },
  totals: { orders: 1_290, revenue: 3_620_000 },
  summary: {
    paid: ZERO, paid_confirmed: ZERO, paid_likely: ZERO, organic: ZERO,
    manager: ZERO, pixel_only: ZERO, unknown: ZERO,
  },
  by_platform: {},
  by_traffic_type: {},
  new_customers: {
    total: 0,
    summary: {
      paid: 0, paid_confirmed: 0, paid_likely: 0, organic: 0,
      manager: 0, pixel_only: 0, unknown: 0,
    },
    by_platform: {},
    by_traffic_type: {},
  },
}

// The block reads both endpoints itself, so the story seeds the cache under
// the keys the hooks will ask for — the filter store's own query string.
function Seeded({ data, children }: { data: TrafficOrganicResponse; children: ReactNode }) {
  const params = useQueryParams()
  const [client] = useState(() => {
    const c = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity } },
    })
    c.setQueryData(queryKeys.trafficOrganic(params), data)
    c.setQueryData(queryKeys.trafficAnalytics(params), ANALYTICS)
    return c
  })
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}

const meta = {
  title: 'Traffic/OrganicBreakdown',
  component: OrganicBreakdown,
} satisfies Meta<typeof OrganicBreakdown>

export default meta
type Story = StoryObj<typeof meta>

/** Click a channel with an arrow to see the letters, story tags or sites inside it. */
export const Default: Story = {
  render: () => (
    <Seeded data={organic(CHANNELS)}>
      <OrganicBreakdown />
    </Seeded>
  ),
}

/** A channel that sold last period and nothing in this one is still listed. */
export const AChannelThatStopped: Story = {
  render: () => (
    <Seeded data={organic([
      ...CHANNELS.filter((c) => c.channel !== 'email_campaigns'),
      channel('email_campaigns', 0, 0, 0, [9, 22_000]),
    ])}>
      <OrganicBreakdown />
    </Seeded>
  ),
}

export const Empty: Story = {
  render: () => (
    <Seeded data={organic([])}>
      <OrganicBreakdown />
    </Seeded>
  ),
}
