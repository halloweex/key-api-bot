import { useState, type ReactNode } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SmsSendShortfall } from './SmsSendShortfall'

/**
 * Under a sent campaign in the list: what its send did not reach, and the way
 * to send the rest. Nothing at all for a campaign sent whole.
 *
 * `fetch` is answered from fixtures; nothing leaves the page.
 */

function Harness({ notSent, uncertain, children }: {
  notSent: number; uncertain: number; children: ReactNode
}) {
  const [client] = useState(() => {
    const real = window.fetch.bind(window)
    window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(typeof input === 'string' ? input : input.toString(),
                          window.location.origin)
      if (url.pathname.endsWith('/send-progress')) {
        return new Response(JSON.stringify({
          campaign: 'black-friday-2026-11-26', total: 8403, done: 8403, pending: 0,
          accepted: 8403 - notSent - uncertain, stoplisted: 0, failed: 0,
          uncertain, notSent, sentAt: '2026-11-26T19:52:36+02:00', batchSize: 1000,
        }), { status: 200, headers: { 'Content-Type': 'application/json' } })
      }
      return real(input, init)
    }
    return new QueryClient({ defaultOptions: { queries: { retry: false } } })
  })
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}

const meta = {
  title: 'SMS/SmsSendShortfall',
  component: SmsSendShortfall,
  args: { campaign: 'black-friday-2026-11-26', onSendRest: () => {} },
} satisfies Meta<typeof SmsSendShortfall>

export default meta
type Story = StoryObj<typeof meta>

/** A batch with no answer and 2 403 never sent: the rest can go. */
export const BrokeOff: Story = {
  render: (args) => (
    <Harness notSent={2403} uncertain={1000}><SmsSendShortfall {...args} /></Harness>
  ),
}

/** Only the unanswered batch is left: said, but nothing is offered. */
export const OnlyUncertain: Story = {
  render: (args) => (
    <Harness notSent={0} uncertain={1000}><SmsSendShortfall {...args} /></Harness>
  ),
}
