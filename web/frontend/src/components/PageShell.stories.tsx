import type { Meta, StoryObj } from '@storybook/react-vite'
import { PageShell } from './PageShell'
import { PageHeading } from './PageHeading'
import { Card, CardHeader, CardTitle, CardContent } from './Card'

const meta = {
  title: 'Layout/PageShell',
  component: PageShell,
  parameters: { layout: 'fullscreen' },
} satisfies Meta<typeof PageShell>

export default meta
type Story = StoryObj<typeof meta>

const DemoCard = ({ title }: { title: string }) => (
  <Card>
    <CardHeader><CardTitle>{title}</CardTitle></CardHeader>
    <CardContent><p className="text-sm text-slate-600">Section content</p></CardContent>
  </Card>
)

export const Feature: Story = {
  args: { variant: 'feature', children: null },
  render: (args) => (
    <PageShell {...args}>
      <DemoCard title="First section" />
      <DemoCard title="Second section" />
    </PageShell>
  ),
}

export const Dashboard: Story = {
  args: { variant: 'dashboard', children: null },
  render: (args) => (
    <PageShell {...args}>
      <DemoCard title="Dense rhythm" />
      <DemoCard title="Between chart panels" />
    </PageShell>
  ),
}

export const Admin: Story = {
  args: { variant: 'admin', children: null },
  render: (args) => (
    <PageShell {...args}>
      <PageHeading title="Admin page" subtitle="Narrower centred column" />
      <DemoCard title="Admin content" />
    </PageShell>
  ),
}
