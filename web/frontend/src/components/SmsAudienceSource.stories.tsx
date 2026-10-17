import type { Meta, StoryObj } from '@storybook/react-vite'
import { SmsAudienceSource } from './SmsAudienceSource'

const meta = {
  title: 'Domain/SmsAudienceSource',
  component: SmsAudienceSource,
  args: { followUp: false, onChange: () => {} },
  parameters: {
    docs: {
      description: {
        component:
          "Where a campaign's people come from: a new audience, or the people " +
          'a sent campaign already reached (a follow-up).',
      },
    },
  },
} satisfies Meta<typeof SmsAudienceSource>

export default meta
type Story = StoryObj<typeof meta>

export const NewAudience: Story = {}

export const FollowUp: Story = {
  args: { followUp: true },
}
