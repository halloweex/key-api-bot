import type { Meta, StoryObj } from '@storybook/react-vite'
import { EmptyState } from './EmptyState'

const meta = {
  title: 'Feedback/EmptyState',
  component: EmptyState,
  args: { message: 'No data for this period' },
} satisfies Meta<typeof EmptyState>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const CustomHint: Story = {
  args: {
    message: 'No campaigns yet',
    hint: 'Create your first campaign from the wizard above',
  },
}
