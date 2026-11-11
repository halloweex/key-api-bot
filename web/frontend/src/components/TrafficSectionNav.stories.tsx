import type { Meta, StoryObj } from '@storybook/react-vite'
import { TrafficSectionNav } from './TrafficSectionNav'

const meta = {
  title: 'Traffic/TrafficSectionNav',
  component: TrafficSectionNav,
} satisfies Meta<typeof TrafficSectionNav>

export default meta
type Story = StoryObj<typeof meta>

/** The funnel: the period in words, and that the CRM filters do not apply. */
export const Funnel: Story = { args: { tab: 'funnel' } }

export const Sales: Story = { args: { tab: 'sales' } }

export const Campaigns: Story = { args: { tab: 'campaigns' } }

export const OnAPhone: Story = {
  args: { tab: 'funnel' },
  globals: { viewport: { value: 'mobile2', isRotated: false } },
}
