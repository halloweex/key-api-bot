import type { Meta, StoryObj } from '@storybook/react-vite'
import { SmsSendBudget } from './SmsSendBudget'

const meta = {
  title: 'Domain/SmsSendBudget',
  component: SmsSendBudget,
  args: {
    recipients: 7382,
    parts: 1,
    pricePerPart: 1.28,
    balance: { state: 'known', amount: 12_000 },
  },
  parameters: {
    docs: {
      description: {
        component:
          'Recipients, cost and the TurboSMS balance beside the send confirmation. ' +
          'The server refuses a send the balance does not cover with 2% to spare; ' +
          'this says so before the press.',
      },
    },
  },
} satisfies Meta<typeof SmsSendBudget>

export default meta
type Story = StoryObj<typeof meta>

export const Covered: Story = {}

/** 17 October 2026: 886 ₴ against the default audience. */
export const Short: Story = {
  args: { balance: { state: 'known', amount: 886 } },
}

export const TwoParts: Story = {
  args: { parts: 2, balance: { state: 'known', amount: 12_000 } },
}

export const Unknown: Story = {
  args: { balance: { state: 'unknown' } },
}

export const Loading: Story = {
  args: { balance: { state: 'loading' } },
}

export const NoTextYet: Story = {
  args: { parts: 0 },
}
