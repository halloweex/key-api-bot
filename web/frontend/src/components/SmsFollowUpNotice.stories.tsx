import type { Meta, StoryObj } from '@storybook/react-vite'
import { SmsFollowUpNotice, SmsFollowUpsLine } from './SmsFollowUpNotice'

const meta = {
  title: 'Domain/SmsFollowUpNotice',
  component: SmsFollowUpNotice,
  args: { root: 'oct-promo', context: 'send', recipients: 6643 },
  parameters: {
    docs: {
      description: {
        component:
          'What a follow-up is, wherever one is handled: the same people messaged ' +
          'twice, with no control of its own, read on the campaign it follows.',
      },
    },
  },
} satisfies Meta<typeof SmsFollowUpNotice>

export default meta
type Story = StoryObj<typeof meta>

/** Above the name field in the send dialog, and in the wizard. */
export const BeforeSending: Story = {}

/** The follow-up's own results, which compare nothing. */
export const OnItsResults: Story = {
  args: { context: 'results', recipients: undefined, onOpenRoot: () => {} },
}

/** On the campaign that was followed up. */
export const FollowedUpBy: Story = {
  render: () => (
    <SmsFollowUpsLine
      followUps={[
        { campaign: 'oct-promo-last-day', sentAt: '2026-10-17T18:00:00+03:00',
          recipients: 6610 },
      ]}
    />
  ),
}
