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

type Roster = { accepted: number; uncertain: number; notSent: number; failed: number }

/** How far a send has got, a batch of 1 000 every ~4.5 s, as the roster says;
 *  once it has answered, what it left behind (`after`). */
function fakeProgress(startedAt: number, after: Roster | null) {
  const total = CAMPAIGN.target
  if (after) {
    return { campaign: CAMPAIGN.campaign, total, done: total, pending: 0, stoplisted: 0,
             sentAt: null, batchSize: 1000, ...after }
  }
  const done = Math.min(total, Math.floor((Date.now() - startedAt) / 4500) * 1000)
  return { campaign: CAMPAIGN.campaign, total, done, pending: total - done, accepted: done,
           stoplisted: 0, failed: 0, uncertain: 0, notSent: 0, sentAt: null, batchSize: 1000 }
}

function installFakeApi(refusal: Record<string, unknown>, status: number, after: Roster | null) {
  const real = window.fetch.bind(window)
  let startedAt = Date.now()
  let answered = after !== null && status === 200
  window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(typeof input === 'string' ? input : input.toString(), window.location.origin)
    if (url.pathname === '/api/customers/sms/channels') {
      return json(200, { sms: true, viber: false, smsSender: 'KoreanStory', viberSender: null,
                         pricePerPart: 1.28, balance: 12_500 })
    }
    if (url.pathname.endsWith('/send-progress')) {
      return json(200, fakeProgress(startedAt, answered ? after : null))
    }
    if (url.pathname.endsWith('/send-rest')) {
      const sent = after?.notSent ?? 0
      after = after && { ...after, accepted: after.accepted + sent, notSent: 0 }
      return json(200, { campaign: CAMPAIGN.campaign, channel: 'sms', accepted: sent,
                         stoplisted: 0, failed: 0, notSent: 0, unsent: 0, partialError: null })
    }
    if (url.pathname.endsWith('/send')) {
      startedAt = Date.now()
      // A send that never answers stays "under way", so the bar can be watched.
      if (status === 0) return new Promise<Response>(() => {})
      answered = true
      return json(status, refusal)
    }
    return real(input, init)
  }
}

function Harness({ refusal, status = 409, after = null, children }: {
  refusal: Record<string, unknown>; status?: number; after?: Roster | null
  children: ReactNode
}) {
  const [client] = useState(() => {
    installFakeApi(refusal, status, after)
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

/**
 * The send under way: the bar fills a batch of 1 000 at a time, read off the
 * roster, while the request itself answers only at the end. Type the name and
 * press send to watch it.
 */
export const Sending: Story = {
  render: (args) => (
    <Harness refusal={{}} status={0}>
      <SmsSendDialog {...args} />
    </Harness>
  ),
}

/**
 * A batch went up and no answer came back (2026-11-26, a read timeout). The
 * dialog says, in place, who is confirmed, who may have the message and who
 * was never sent, and offers only to close.
 */
export const BrokeHalfway: Story = {
  render: (args) => (
    <Harness status={502} after={{ accepted: 2000, uncertain: 1000, notSent: 635, failed: 0 }}
             refusal={{
      detail: 'TurboSMS request failed: ReadTimeout. 2000 recipients are confirmed sent …',
      reason: 'gateway_outcome_unknown', error: 'TurboSMS request failed: ReadTimeout',
      accepted: 2000, uncertain: 1000, notSent: 635,
    }}>
      <SmsSendDialog {...args} />
    </Harness>
  ),
}

/**
 * Opened from the campaign list after a reload, for the rest of a send that
 * broke off: the campaign's own text, only the never-sent, the name typed back.
 */
export const SendTheRest: Story = {
  args: { mode: 'rest', campaign: { ...CAMPAIGN, sentAt: '2026-11-26T17:52:36Z',
                                    messageText: '🖤BLACK FRIDAY до -50%! Лише до 30.11 на koreanstory.com.ua',
                                    messageParts: 1 } },
  render: (args) => (
    <Harness status={200} refusal={{}}
             after={{ accepted: 2000, uncertain: 1000, notSent: 635, failed: 0 }}>
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
