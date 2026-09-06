import type { Meta, StoryObj } from '@storybook/react-vite'
import { Wrapper } from './Wrapper'
import { Button } from './Button'
import { Badge } from './Badge'

const meta = {
  title: 'Layout/Wrapper',
  component: Wrapper,
  parameters: {
    docs: {
      description: {
        component:
          'The project-wide layout primitive — the only component allowed to carry ' +
          'padding/gap/margin/flex props. Everything visual lives inside the other components; ' +
          'Wrapper only decides how siblings sit next to each other.',
      },
    },
  },
} satisfies Meta<typeof Wrapper>

export default meta
type Story = StoryObj<typeof meta>

const Box = ({ label }: { label: string }) => (
  <div className="rounded-lg bg-purple-100 text-purple-700 text-sm font-medium px-4 py-2">
    {label}
  </div>
)

export const Row: Story = {
  args: { children: null, dir: 'row', gap: 'md' },
  render: (args) => (
    <Wrapper {...args}>
      <Box label="One" />
      <Box label="Two" />
      <Box label="Three" />
    </Wrapper>
  ),
}

export const Column: Story = {
  args: { children: null, dir: 'column', gap: 'sm' },
  render: (args) => (
    <Wrapper {...args}>
      <Box label="First" />
      <Box label="Second" />
      <Box label="Third" />
    </Wrapper>
  ),
}

export const RowResponsive: Story = {
  name: 'Row (responsive)',
  args: { children: null, dir: 'row-responsive', gap: 'md' },
  render: (args) => (
    <Wrapper {...args}>
      <Box label="Stacks on mobile" />
      <Box label="Row on sm+" />
    </Wrapper>
  ),
}

export const SplitRow: Story = {
  name: 'Row with justify=between',
  args: { children: null, dir: 'row', align: 'center', justify: 'between' },
  render: (args) => (
    <Wrapper {...args}>
      <Badge tone="purple">Left</Badge>
      <Button size="sm">Right action</Button>
    </Wrapper>
  ),
}
