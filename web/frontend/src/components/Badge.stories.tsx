import type { Meta, StoryObj } from '@storybook/react-vite'
import { AlertTriangle, Check } from 'lucide-react'
import { Badge } from './Badge'
import { Wrapper } from './Wrapper'

const meta = {
  title: 'Primitives/Badge',
  component: Badge,
  args: { children: 'Badge' },
} satisfies Meta<typeof Badge>

export default meta
type Story = StoryObj<typeof meta>

const TONES = [
  'neutral', 'slate', 'green', 'red', 'blue', 'purple',
  'orange', 'yellow', 'cyan', 'indigo', 'rose', 'teal',
] as const

export const Tones: Story = {
  render: () => (
    <Wrapper dir="row" gap="sm" wrap>
      {TONES.map((tone) => (
        <Badge key={tone} tone={tone}>{tone}</Badge>
      ))}
    </Wrapper>
  ),
}

export const Shapes: Story = {
  render: () => (
    <Wrapper dir="row" gap="sm" align="center">
      <Badge tone="purple" shape="pill">pill</Badge>
      <Badge tone="purple" shape="tag">tag</Badge>
      <Badge tone="purple" shape="square">square</Badge>
    </Wrapper>
  ),
}

export const WithIcon: Story = {
  render: () => (
    <Wrapper dir="row" gap="sm" align="center">
      <Badge tone="red" icon={AlertTriangle}>3 alerts</Badge>
      <Badge tone="green" icon={Check}>Sent</Badge>
    </Wrapper>
  ),
}
