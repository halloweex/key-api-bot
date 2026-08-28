import type { Meta, StoryObj } from '@storybook/react-vite'
import { ChartGrid } from './ChartGrid'
import { Card, CardHeader, CardTitle, CardContent } from './Card'

const meta = {
  title: 'Layout/ChartGrid',
  component: ChartGrid,
} satisfies Meta<typeof ChartGrid>

export default meta
type Story = StoryObj<typeof meta>

const Panel = ({ title }: { title: string }) => (
  <Card>
    <CardHeader><CardTitle>{title}</CardTitle></CardHeader>
    <CardContent><div className="h-32 rounded-lg bg-slate-100" /></CardContent>
  </Card>
)

export const Comfortable: Story = {
  args: { children: null },
  render: () => (
    <ChartGrid>
      <Panel title="Orders by Source" />
      <Panel title="Revenue by Source" />
    </ChartGrid>
  ),
}

export const Dense: Story = {
  args: { children: null, density: 'dense' },
  render: () => (
    <ChartGrid density="dense">
      <Panel title="Top Products" />
      <Panel title="Top by Revenue" />
    </ChartGrid>
  ),
}
