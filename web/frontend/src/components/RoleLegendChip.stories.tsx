import type { Meta, StoryObj } from '@storybook/react-vite'
import { RoleLegendChip } from './RoleLegendChip'
import { Wrapper } from './Wrapper'

const meta = {
  title: 'Data display/RoleLegendChip',
  component: RoleLegendChip,
} satisfies Meta<typeof RoleLegendChip>

export default meta
type Story = StoryObj<typeof meta>

export const Legend: Story = {
  args: { tone: 'purple', label: 'Admin' },
  render: () => (
    <Wrapper dir="row" gap="md" wrap>
      <RoleLegendChip tone="purple" label="Admin" description="everything" />
      <RoleLegendChip tone="blue" label="Marketer" description="SMS campaigns" />
      <RoleLegendChip tone="slate" label="Viewer" description="read-only" />
    </Wrapper>
  ),
}
