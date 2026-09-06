import type { Meta, StoryObj } from '@storybook/react-vite'
import { Users, CircleDollarSign, TrendingUp, Clock } from 'lucide-react'
import { TileGrid } from './TileGrid'
import { MetricCard } from './MetricCard'
import { InsightCard } from './InsightCard'

const meta = {
  title: 'Layout/TileGrid',
  component: TileGrid,
  parameters: {
    docs: {
      description: {
        component:
          'The one grid for metric tiles — summary rows, KPI triplets, insight pairs. ' +
          'Gap is fixed at 12px → 16px (sm+): a half-step tighter than the page rhythm, ' +
          'so a row of tiles reads as one block.',
      },
    },
  },
} satisfies Meta<typeof TileGrid>

export default meta
type Story = StoryObj<typeof meta>

export const FourUp: Story = {
  args: { children: null, columns: 4 },
  render: () => (
    <TileGrid columns={4}>
      <MetricCard surface="tile-gradient" tone="blue" icon={Users} label="Customers" value="4,182" />
      <MetricCard surface="tile-gradient" tone="green" icon={CircleDollarSign} label="Revenue" value="₴3.55M" />
      <MetricCard surface="tile-gradient" tone="purple" icon={TrendingUp} label="Repeat rate" value="38.2%" />
      <MetricCard surface="tile-gradient" tone="orange" icon={Clock} label="Days between" value="46" />
    </TileGrid>
  ),
}

export const ThreeUp: Story = {
  args: { children: null, columns: 3 },
  render: () => (
    <TileGrid columns={3}>
      <MetricCard surface="tile" label="Median LTV" value="₴4,120" />
      <MetricCard surface="tile" label="Top decile" value="₴18,300" tone="green" />
      <MetricCard surface="tile" label="Cohorts" value="14" />
    </TileGrid>
  ),
}

export const TwoUpWide: Story = {
  args: { children: null, columns: 2 },
  render: () => (
    <TileGrid columns={2}>
      <InsightCard
        icon={TrendingUp}
        title="Retention trend"
        value="+2.4pp"
        description="Newer cohorts retain better than older ones."
        tone="green"
      />
      <InsightCard
        icon={CircleDollarSign}
        title="Revenue opportunity"
        value="₴52,000/mo"
        description="Closing the M1 gap to the best cohort."
        tone="amber"
      />
    </TileGrid>
  ),
}

export const FiveUp: Story = {
  args: { children: null, columns: 5 },
  render: () => (
    <TileGrid columns={5}>
      {['Paid', 'Organic', 'Manager', 'Pixel', 'Unknown'].map((label, i) => (
        <MetricCard key={label} surface="tile" label={label} value={`₴${(120 - i * 20)}K`} />
      ))}
    </TileGrid>
  ),
}
