import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState, type ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SiteFunnel } from './SiteFunnel'
import { ApiError } from '../api/client'
import { queryKeys } from '../hooks/useApi'
import { useQueryParams } from '../store/filterStore'
import { addDays, kyivToday } from '../utils/smsDates'
import { FUNNEL, STEADY, c } from '../test/funnelFixtures'
import type { SummaryResponse, TrafficFunnelResponse } from '../types/api'

// Illustrative numbers (`test/funnelFixtures`), not production's: built to land
// where a real month does, one state of the block per story.

/** What `useSiteOrders` asks `/api/summary` for these days. */
function siteOrdersKey(data: TrafficFunnelResponse) {
  const p = new URLSearchParams()
  p.set('start_date', data.periods.current.start)
  p.set('end_date', data.periods.current.end)
  p.set('source_id', '4')
  p.set('sales_type', 'all')
  return queryKeys.summary(p.toString())
}

const ORDERS: SummaryResponse = {
  totalOrders: 760, totalRevenue: 0, avgCheck: 0, totalReturns: 0, returnRate: 0, startDate: '', endDate: '',
}

function failed(qc: QueryClient, queryKey: readonly unknown[], error: Error) {
  qc.getQueryCache().build(qc, { queryKey }).setState({
    status: 'error', fetchStatus: 'idle', error, errorUpdatedAt: Date.now(), errorUpdateCount: 1,
  })
}

function Seeded({ data, error, noOrders, children }: {
  data?: TrafficFunnelResponse
  error?: ApiError
  /** The CRM's count of the website's orders did not come. */
  noOrders?: boolean
  children: ReactNode
}) {
  const params = useQueryParams()
  const [client] = useState(() => {
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false, retryOnMount: false, staleTime: Infinity } } })
    if (data) {
      qc.setQueryData(queryKeys.trafficFunnel(params), data)
      if (noOrders) failed(qc, siteOrdersKey(data), new ApiError(503, 'read refused'))
      else qc.setQueryData(siteOrdersKey(data), ORDERS)
    }
    if (error) failed(qc, queryKeys.trafficFunnel(params), error)
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

const show = (data?: TrafficFunnelResponse, error?: ApiError, noOrders?: boolean) => () => (
  <Seeded data={data} error={error} noOrders={noOrders}><SiteFunnel /></Seeded>
)

const PHONE = { viewport: { value: 'mobile2', isRotated: false } }

/** Within noise overall, one step down: the counts lead what is said. */
export const Default: Story = { render: show(FUNNEL) }

/** The same block on a phone: the verdict beside the number, the steps
 *  stacked, the breakdown as rows of conversion and what it was. */
export const OnAPhone: Story = { render: show(FUNNEL), globals: PHONE }

/** Nothing beyond noise. */
export const Steady: Story = {
  render: show({ ...FUNNEL, total: { ...FUNNEL.total, previous: STEADY } }),
}

/** The period before predates what GA4 keeps, so the year ago is the
 *  comparison — and the conversion rose, part of it from the mix. */
const A_YEAR_AGO = { ...FUNNEL, periods: { ...FUNNEL.periods, previous: { start: '2025-08-01', end: '2025-08-31' } } }
export const BetterThanAYearAgo: Story = { render: show(A_YEAR_AGO) }

/** The longest the block gets on a phone: a verdict, the strongest step and
 *  the mix said in shares. */
export const BetterThanAYearAgoOnAPhone: Story = { render: show(A_YEAR_AGO), globals: PHONE }

/** A year ago is older than GA4 keeps: that choice is off, and says why. */
export const NoYearAgo: Story = {
  render: show({
    ...FUNNEL,
    periods: { ...FUNNEL.periods, year_ago: { start: '2025-03-01', end: '2025-03-31' } },
    total: { ...FUNNEL.total, year_ago: c(0, 0, 0, 0, 0) },
  }),
}

/** One day: too few people to tell a change from chance. */
const ONE_DAY: TrafficFunnelResponse = {
  ...FUNNEL,
  periods: {
    current: { start: '2026-10-31', end: '2026-10-31' },
    previous: { start: '2026-10-30', end: '2026-10-30' },
    year_ago: { start: '2025-10-31', end: '2025-10-31' },
  },
  total: { current: c(1100, 600, 80, 35, 23), previous: c(1050, 580, 75, 33, 22), year_ago: c(2100, 1000, 85, 38, 24) },
}
export const FewPeople: Story = { render: show(ONE_DAY) }

export const FewPeopleOnAPhone: Story = { render: show(ONE_DAY), globals: PHONE }

/** The period runs to today: GA4 is still counting its last days. */
const TO_TODAY = { ...FUNNEL, periods: { ...FUNNEL.periods, current: { start: addDays(kyivToday(), -30), end: kyivToday() } } }
export const StillCounting: Story = { render: show(TO_TODAY) }

/** Two notices at once, a line each in one box: GA4 did not answer, and the
 *  days it gave are still being counted. */
export const StaleAndStillCountingOnAPhone: Story = { render: show({ ...TO_TODAY, stale: true }), globals: PHONE }

/** GA4 did not answer; the last figures it gave are on screen. */
export const Stale: Story = { render: show({ ...FUNNEL, stale: true }) }

/** The CRM's count did not come: the line still says the funnel counts
 *  people, and why the two differ is still a tap away. */
export const NoCrmCount: Story = { render: show(FUNNEL, undefined, true) }

export const NotConfigured: Story = { render: show(undefined, new ApiError(503, 'GA4 is not configured')) }

export const Unavailable: Story = { render: show(undefined, new ApiError(503, 'GA4 is unavailable')) }

export const Backwards: Story = {
  render: show({ ...FUNNEL, periods: { ...FUNNEL.periods, current: { start: '2026-10-31', end: '2026-10-01' } } }),
}

export const NobodyCame: Story = {
  render: show({ ...FUNNEL, total: { ...FUNNEL.total, current: c(0, 0, 0, 0, 0) } }),
}
