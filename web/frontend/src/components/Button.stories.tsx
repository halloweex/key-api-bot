import type { Meta, StoryObj } from '@storybook/react-vite'
import { Button } from './Button'
import { Wrapper } from './Wrapper'

const meta = {
  title: 'Primitives/Button',
  component: Button,
  args: { children: 'Button' },
} satisfies Meta<typeof Button>

export default meta
type Story = StoryObj<typeof meta>

export const Primary: Story = {
  args: { variant: 'primary', children: 'Save changes' },
}

export const Secondary: Story = {
  args: { variant: 'secondary', children: 'Cancel' },
}

export const Ghost: Story = {
  args: { variant: 'ghost', children: 'Dismiss' },
}

export const Disabled: Story = {
  args: { variant: 'primary', children: 'Unavailable', disabled: true },
}

export const Sizes: Story = {
  render: () => (
    <Wrapper dir="row" gap="md" align="center">
      <Button size="sm">Small</Button>
      <Button size="md">Medium</Button>
      <Button size="lg">Large</Button>
      <Button size="pill">Pill (filter chip size)</Button>
    </Wrapper>
  ),
}

export const FullWidth: Story = {
  args: { variant: 'primary', fullWidth: true, children: 'Stretch to parent' },
}
