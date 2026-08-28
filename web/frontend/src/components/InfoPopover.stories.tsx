import type { Meta, StoryObj } from '@storybook/react-vite'
import { InfoPopover } from './InfoPopover'

const meta = {
  title: 'Data display/InfoPopover',
  component: InfoPopover,
} satisfies Meta<typeof InfoPopover>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: {
    title: 'Repeat rate',
    children: 'Share of customers in the period who had ordered at least once before.',
  },
  render: (args) => (
    <div className="flex items-center gap-1 text-sm text-slate-600">
      Repeat rate <InfoPopover {...args} />
    </div>
  ),
}

export const Wide: Story = {
  args: {
    title: 'Forecast',
    size: 'wide',
    children:
      'LightGBM model trained on ~780 days of history. Thursday and Friday carry a known ' +
      'underprediction bias because promo spikes are unpredictable from lagged features.',
  },
  render: (args) => (
    <div className="flex items-center gap-1 text-sm text-slate-600">
      Predicted <InfoPopover {...args} />
    </div>
  ),
}
