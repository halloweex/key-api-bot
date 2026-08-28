import type { Meta, StoryObj } from '@storybook/react-vite'
import { FlaskConical, Info } from 'lucide-react'
import { InfoBanner } from './InfoBanner'

const meta = {
  title: 'Data display/InfoBanner',
  component: InfoBanner,
} satisfies Meta<typeof InfoBanner>

export default meta
type Story = StoryObj<typeof meta>

export const Plain: Story = {
  args: {
    children: 'Revenue excludes cancelled and lost orders (KeyCRM group 6).',
  },
}

export const WithTitleAndIcon: Story = {
  args: {
    icon: FlaskConical,
    title: 'How it works',
    children:
      'A holdout group is kept aside so the campaign result can be measured against people who received nothing.',
  },
}

export const RichContent: Story = {
  args: {
    icon: Info,
    title: 'How permissions work',
    children: (
      <ul className="list-disc ml-4 space-y-1">
        <li>View — the page and its numbers</li>
        <li>Edit — actions that change data</li>
      </ul>
    ),
  },
}
