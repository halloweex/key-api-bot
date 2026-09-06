import type { Meta, StoryObj } from '@storybook/react-vite'
import { ChartContainer } from './ChartContainer'
import { InfoPopover } from './InfoPopover'
import { Badge } from './Badge'

const FakeChart = () => (
  <div className="h-[300px] flex items-end gap-2">
    {[40, 70, 55, 90, 65, 80, 30].map((h, i) => (
      <div key={i} className="flex-1 bg-purple-300 rounded-t" style={{ height: `${h}%` }} />
    ))}
  </div>
)

const meta = {
  title: 'Layout/ChartContainer',
  component: ChartContainer,
  args: { title: 'Revenue Trend', children: <FakeChart /> },
} satisfies Meta<typeof ChartContainer>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const WithHeaderExtras: Story = {
  args: {
    titleExtra: <InfoPopover title="Revenue">Excludes cancelled orders.</InfoPopover>,
    action: <Badge tone="purple">Predicted: ₴1.2M</Badge>,
  },
}

export const Loading: Story = {
  args: { isLoading: true },
}

export const ErrorState: Story = {
  args: { error: new Error('boom'), onRetry: () => {} },
}

export const Empty: Story = {
  args: { isEmpty: true, height: 'md' },
}
