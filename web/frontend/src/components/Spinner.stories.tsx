import type { Meta, StoryObj } from '@storybook/react-vite'
import { Spinner } from './Spinner'

const meta = {
  title: 'Feedback/Spinner',
  component: Spinner,
} satisfies Meta<typeof Spinner>

export default meta
type Story = StoryObj<typeof meta>

export const Fill: Story = {
  args: { variant: 'fill' },
  render: (args) => (
    <div className="flex h-48 border border-dashed border-slate-300 rounded-lg">
      <Spinner {...args} />
    </div>
  ),
}

export const Screen: Story = {
  args: { variant: 'screen' },
  parameters: { layout: 'fullscreen' },
}
