import type { Meta, StoryObj } from '@storybook/react-vite'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { AccessRequestsCard } from './AccessRequestsCard'
import type { AccessPreset } from '../types/api'

const presets: AccessPreset[] = [
  { key: 'full', features: ['dashboard', 'products', 'traffic', 'inventory', 'reports', 'marketing', 'margin', 'expenses', 'sms'] },
  { key: 'standard', features: ['dashboard', 'products', 'traffic', 'inventory', 'reports', 'marketing'] },
  { key: 'traffic_only', features: ['traffic'] },
  { key: 'marketing', features: ['marketing', 'traffic', 'reports'] },
]

// The card reads its queue itself, so the story seeds the cache rather than
// passing rows in: that is the shape the page uses, and a prop-driven variant
// would document an API the component does not have.
function withQueue(requests: unknown[]) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchInterval: false } },
  })
  client.setQueryData(['accessRequests'], { requests, count: requests.length })
  return function Wrapped() {
    return (
      <QueryClientProvider client={client}>
        <AccessRequestsCard presets={presets} />
      </QueryClientProvider>
    )
  }
}

const meta = {
  title: 'Admin/AccessRequestsCard',
  component: AccessRequestsCard,
} satisfies Meta<typeof AccessRequestsCard>

export default meta
type Story = StoryObj<typeof meta>

export const OneWaiting: Story = {
  render: withQueue([
    {
      user_id: 481_516_234,
      username: 'newcomer',
      first_name: 'Олена',
      last_name: 'К.',
      requested_at: '2026-09-08T05:00:00Z',
      denial_count: 0,
    },
  ]),
}

/** Somebody who has been refused before — the count is what freezes them. */
export const RefusedBefore: Story = {
  render: withQueue([
    {
      user_id: 481_516_235,
      username: 'persistent',
      first_name: 'Persistent',
      last_name: null,
      requested_at: '2026-09-08T04:00:00Z',
      denial_count: 3,
    },
  ]),
}

/** Nothing waiting: the card renders nothing at all, rather than an empty box
 *  that teaches admins to stop looking at this part of the page. */
export const Empty: Story = {
  render: withQueue([]),
}
