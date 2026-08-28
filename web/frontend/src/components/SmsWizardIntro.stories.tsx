import type { Meta, StoryObj } from '@storybook/react-vite'
import { SmsWizardIntro } from './SmsCampaignWizard'

const meta = {
  title: 'Domain/SmsWizardIntro',
  component: SmsWizardIntro,
  args: { onStart: () => {} },
  parameters: {
    docs: {
      description: {
        component:
          "Step 1's collapsed state on the SMS page: the panel header with the " +
          '"New campaign" action, shown while the wizard is closed so the page ' +
          'always reads Step 1 → 2 → 3.',
      },
    },
  },
} satisfies Meta<typeof SmsWizardIntro>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}
