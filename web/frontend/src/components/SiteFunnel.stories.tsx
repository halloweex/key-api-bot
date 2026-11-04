import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState, type ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SiteFunnel } from './SiteFunnel'
import { queryKeys } from '../hooks/useApi'
import { useQueryParams } from '../store/filterStore'
import type { FunnelCounts, FunnelRow, TrafficFunnelResponse } from '../types/api'

// Illustrative numbers in the shape GA4 answered on 2026-11-04, not production's.

const c = (e: number, v: number, a: number, b: number, p: number): FunnelCounts => ({
  entered: e, viewed_product: v, added_to_cart: a, began_checkout: b, purchased: p,
})
const row = (key: string, now: FunnelCounts, prev: FunnelCounts, ago: FunnelCounts, type?: FunnelRow['type']): FunnelRow => ({
  key, type, current: now, previous: prev, year_ago: ago,
})

const DATA: TrafficFunnelResponse = {
  periods: {
    current: { start: '2026-10-01', end: '2026-10-31' },
    previous: { start: '2026-08-31', end: '2026-09-30' },
    year_ago: { start: '2025-10-01', end: '2025-10-31' },
  },
  earliest: '2025-09-04',
  steps: ['entered', 'viewed_product', 'added_to_cart', 'began_checkout', 'purchased'],
  total: {
    current: c(33600, 18900, 2400, 1080, 730),
    previous: c(30300, 16000, 2050, 960, 640),
    year_ago: c(65200, 25800, 2580, 1110, 670),
  },
  by_type: [
    row('cold', c(14800, 9500, 700, 330, 220), c(13900, 8800, 640, 300, 200), c(38000, 14100, 900, 380, 230)),
    row('warm', c(15200, 7600, 1450, 640, 440), c(13300, 6200, 1220, 570, 380), c(22500, 9700, 1450, 640, 380)),
    row('other', c(3600, 1800, 250, 110, 70), c(3100, 1000, 190, 90, 60), c(4700, 2000, 230, 90, 60)),
  ],
  by_channel: [
    row('Paid Social', c(8900, 5900, 380, 170, 115), c(8100, 5200, 330, 150, 100), c(30000, 10600, 600, 250, 150), 'cold'),
    row('Organic Social', c(7600, 3500, 720, 310, 205), c(6800, 3000, 610, 270, 180), c(9000, 3700, 600, 250, 150), 'warm'),
    row('Cross-network', c(4600, 2700, 280, 135, 90), c(4500, 2600, 270, 130, 85), c(6000, 2800, 250, 110, 70), 'cold'),
    row('Direct', c(4300, 2300, 360, 190, 140), c(3900, 2000, 320, 170, 120), c(5600, 2500, 380, 190, 120), 'warm'),
    row('Unassigned', c(3500, 2250, 240, 120, 80), c(3000, 1800, 190, 100, 65), c(4600, 1900, 220, 90, 55), 'other'),
  ],
  by_device: [
    row('mobile', c(27500, 15400, 1800, 780, 510), c(24800, 13000, 1530, 690, 450), c(55000, 21500, 1950, 800, 470)),
    row('desktop', c(5700, 3300, 570, 290, 210), c(5200, 2850, 500, 260, 180), c(9600, 4100, 600, 300, 190)),
  ],
  stale: false,
  fetched_at: '2026-11-04T08:30:00Z',
}

function Seeded({ data, children }: { data: TrafficFunnelResponse; children: ReactNode }) {
  const params = useQueryParams()
  const [client] = useState(() => {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } })
    qc.setQueryData(queryKeys.trafficFunnel(params), data)
    return qc
  })
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}

const meta = {
  title: 'Traffic/SiteFunnel',
  component: SiteFunnel,
} satisfies Meta<typeof SiteFunnel>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  render: () => (
    <Seeded data={DATA}>
      <SiteFunnel />
    </Seeded>
  ),
}

/** A period whose year-ago dates GA4 no longer holds: no comparison is drawn. */
export const NoYearAgo: Story = {
  render: () => (
    <Seeded data={{
      ...DATA,
      periods: { ...DATA.periods, year_ago: { start: '2025-03-01', end: '2025-03-31' } },
      total: { ...DATA.total, year_ago: c(0, 0, 0, 0, 0) },
    }}>
      <SiteFunnel />
    </Seeded>
  ),
}
