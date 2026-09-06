import type { Meta, StoryObj } from '@storybook/react-vite'
import { TrendingUp, TrendingDown, DollarSign, Activity } from 'lucide-react'
import { InsightCard } from './InsightCard'

const meta = {
  title: 'Data display/InsightCard',
  component: InsightCard,
  args: {
    icon: TrendingUp,
    title: 'Retention trend',
    value: '+2.4pp',
    description: 'Newer cohorts retain better than older ones.',
  },
} satisfies Meta<typeof InsightCard>

export default meta
type Story = StoryObj<typeof meta>

export const Tones: Story = {
  render: () => (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
      <InsightCard
        icon={TrendingUp}
        title="Retention trend"
        value="+2.4pp"
        description="Newer cohorts retain better than older ones."
        tone="green"
      />
      <InsightCard
        icon={TrendingDown}
        title="Retention trend"
        value="-1.8pp"
        description="Newer cohorts retain worse than older ones."
        tone="red"
      />
      <InsightCard
        icon={DollarSign}
        title="Revenue opportunity"
        value="₴52,000/mo potential"
        description="Closing the M1 gap to the best cohort is worth this much."
        tone="amber"
      />
      <InsightCard
        icon={Activity}
        title="Decay profile"
        value="Half-life: M3"
        description="Half of a cohort is gone by month three."
        subtext="M1→M3 drop: 14pp"
        tone="neutral"
      />
    </div>
  ),
}
