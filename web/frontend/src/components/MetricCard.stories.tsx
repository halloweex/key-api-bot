import type { Meta, StoryObj } from '@storybook/react-vite'
import { AlertTriangle, TrendingUp, Users } from 'lucide-react'
import { MetricCard } from './MetricCard'
import { Badge } from './Badge'

const meta = {
  title: 'Data display/MetricCard',
  component: MetricCard,
  args: { label: 'Repeat rate', value: '38.2%' },
} satisfies Meta<typeof MetricCard>

export default meta
type Story = StoryObj<typeof meta>

export const Surfaces: Story = {
  render: () => (
    <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
      <MetricCard surface="card" label="card" value="₴128,400" tone="green" />
      <MetricCard surface="tile" label="tile" value="1,284" />
      <div className="bg-slate-800 rounded-xl p-3">
        <MetricCard surface="tile-dark" label="tile-dark" value="27.6%" tone="cyan" />
      </div>
      <MetricCard surface="tile-tinted" label="tile-tinted" value="₴86,200" tone="purple" />
      <MetricCard surface="tile-gradient" label="tile-gradient" value="412" tone="orange" />
    </div>
  ),
}

export const WithIconBadge: Story = {
  name: 'Icon (badge style)',
  args: {
    surface: 'card',
    tone: 'blue',
    icon: Users,
    label: 'Unique customers',
    value: '4,182',
    sub: 'last 30 days',
  },
}

export const WithIconWatermark: Story = {
  name: 'Icon (watermark style)',
  args: {
    surface: 'tile-gradient',
    tone: 'red',
    icon: AlertTriangle,
    iconStyle: 'watermark',
    label: 'At-risk customers',
    value: '312',
    sub: '18% of total',
  },
}

export const WithEmoji: Story = {
  args: {
    surface: 'tile-gradient',
    tone: 'purple',
    emoji: '📣',
    label: 'Marketing',
    value: '₴42,000',
  },
}

export const WithValueExtra: Story = {
  args: {
    surface: 'card',
    tone: 'green',
    icon: TrendingUp,
    label: 'Revenue',
    value: '₴1,834,807',
    valueExtra: <Badge tone="green">+12.4%</Badge>,
  },
}
