import { useState, type ReactNode } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SmsSendDialog } from './SmsSendDialog'
import { ToastProvider } from './Toast'
import type { SmsCampaignSummary } from '../types/api'

/**
 * The send: the text, who gets it, what it costs against the gateway's
 * balance, and the campaign's name typed back.
 *
 * Some refusals only the server can make — another campaign messaged these
 * people today, a balance that moved since the dialog opened. They come back
 * as a 409 whose English `detail` is for API callers and whose `reason` and
 * figures the dialog says in the reader's language (`utils/smsRefusal.ts`).
 * Type the name below and press send to see one.
 *
 * `fetch` is answered from fixtures; nothing leaves the page.
 */

const CAMPAIGN: SmsCampaignSummary = {
  campaign: 'sms-2026-10-18', ltvBasis: 'margin', salesType: 'retail', holdoutPct: 10,
  promocode: null, exportedAt: '2026-10-18T09:00:00Z', sentAt: null, notes: null,
  members: 4_064, target: 3_635, holdout: 429, followUpOf: null,
}

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status, headers: { 'Content-Type': 'application/json' },
  })
}

function installFakeApi(refusal: Record<string, unknown>) {
  const real = window.fetch.bind(window)
  window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(typeof input === 'string' ? input : input.toString(), window.location.origin)
    if (url.pathname === '/api/customers/sms/channels') {
      return json(200, { sms: true, viber: false, smsSender: 'KoreanStory', viberSender: null,
                         pricePerPart: 1.28, balance: 12_500 })
    }
    if (url.pathname.endsWith('/send')) return json(409, refusal)
    return real(input, init)
  }
}

function Harness({ refusal, children }: { refusal: Record<string, unknown>; children: ReactNode }) {
  const [client] = useState(() => {
    installFakeApi(refusal)
    return new QueryClient({ defaultOptions: { queries: { retry: false } } })
  })
  return (
    <QueryClientProvider client={client}>
      <ToastProvider>{children}</ToastProvider>
    </QueryClientProvider>
  )
}

const meta = {
  title: 'SMS/SmsSendDialog',
  component: SmsSendDialog,
  args: {
    campaign: CAMPAIGN,
    initialText: 'KoreanStory: -15% на весь догляд до неділі. koreanstory.com.ua/sale',
    onClose: () => {},
  },
} satisfies Meta<typeof SmsSendDialog>

export default meta
type Story = StoryObj<typeof meta>

/**
 * Today's other campaign reached some of these people — three of them its
 * control group. The server refuses after the claim and hands it back.
 */
export const RefusedOtherCampaignToday: Story = {
  render: (args) => (
    <Harness refusal={{
      detail: "refused: this roster overlaps campaigns sent in the last 24 h — 3 in the control arm of 'oct-vip' …",
      reason: 'recent_campaign', hours: 24,
      conflicts: [
        { campaign: 'oct-vip', arm: 'control', people: 3, sentAt: '2026-10-18T06:05:00+00:00' },
        { campaign: 'oct-all', arm: 'target', people: 120, sentAt: '2026-10-18T05:40:00+00:00' },
      ],
    }}>
      <SmsSendDialog {...args} />
    </Harness>
  ),
}

/** The balance moved after the dialog opened: refused with the figures. */
export const RefusedBalanceShort: Story = {
  render: (args) => (
    <Harness refusal={{
      detail: 'the TurboSMS balance is 4650.00 ₴ and this send costs up to 4746.12 ₴ …',
      reason: 'balance_short', balance: 4650, cost: 4746.12, shortfall: 96.12,
      recipients: 3635, parts: 1, pricePerPart: 1.28, margin: 1.02,
    }}>
      <SmsSendDialog {...args} />
    </Harness>
  ),
}
