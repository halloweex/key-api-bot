import type { Meta, StoryObj } from '@storybook/react-vite'
import { SmsFollowUpPicker } from './SmsFollowUpPicker'
import type { SmsCampaignSummary } from '../types/api'

const sent = (campaign: string, sentAt: string, target: number): SmsCampaignSummary => ({
  campaign, ltvBasis: 'margin', salesType: 'retail', holdoutPct: 10,
  promocode: null, exportedAt: sentAt, sentAt, notes: null,
  members: Math.round(target / 0.9), target, holdout: Math.round(target / 9),
  recipientsSent: target - 12, followUpOf: null,
})

const CANDIDATES = [
  sent('oct-promo', '2026-10-17T10:00:00+03:00', 6643),
  sent('oct-vip', '2026-10-14T11:30:00+03:00', 1180),
]

const meta = {
  title: 'Domain/SmsFollowUpPicker',
  component: SmsFollowUpPicker,
  args: { candidates: CANDIDATES, value: null, onChange: () => {} },
  parameters: {
    docs: {
      description: {
        component:
          "A follow-up's whole audience: the sent campaign it follows. Window, " +
          'tiers, filters and a control are absent on purpose — the server ' +
          'refuses them beside a follow-up.',
      },
    },
  },
} satisfies Meta<typeof SmsFollowUpPicker>

export default meta
type Story = StoryObj<typeof meta>

export const NothingPicked: Story = {}

export const Picked: Story = {
  args: { value: 'oct-promo', recipients: 6631 },
}

export const NothingToFollowUp: Story = {
  args: { candidates: [] },
}
