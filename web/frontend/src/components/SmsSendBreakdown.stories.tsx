import type { Meta, StoryObj } from '@storybook/react-vite'
import { SmsSendBreakdown } from './SmsSendBreakdown'
import type { SmsSendProgress } from '../types/api'

/**
 * Where a campaign's send stands, by what the gateway said about each
 * recipient. The bar keeps apart who got the message, who the gateway refused,
 * the batch it gave no answer for (they may have it, and are never sent
 * again) and who was never sent (the rest that can be sent).
 */

function roster(over: Partial<SmsSendProgress>): SmsSendProgress {
  return {
    campaign: 'black-friday-2026-11-26', total: 8403, done: 0, pending: 8403,
    accepted: 0, stoplisted: 0, failed: 0, uncertain: 0, notSent: 0,
    sentAt: '2026-11-26T19:52:36+02:00', batchSize: 1000, ...over,
  }
}

const meta = {
  title: 'SMS/SmsSendBreakdown',
  component: SmsSendBreakdown,
  args: { total: 8403, running: false },
} satisfies Meta<typeof SmsSendBreakdown>

export default meta
type Story = StoryObj<typeof meta>

/** Batch 4 of 9 going: 3 000 answered, about half a minute left. */
export const Running: Story = {
  args: { running: true, progress: roster({ done: 3000, accepted: 2980, failed: 20, pending: 5403 }) },
}

/** The shape of 2026-11-26: a batch with no answer, and the rest never sent. */
export const BrokeOff: Story = {
  args: {
    progress: roster({ done: 8403, pending: 0, accepted: 4935, failed: 49, stoplisted: 16,
                       uncertain: 1000, notSent: 2403 }),
  },
}

/** Sent whole: only who received it, and the gateway's refusals. */
export const Complete: Story = {
  args: { progress: roster({ done: 8403, pending: 0, accepted: 8334, failed: 69 }) },
}
