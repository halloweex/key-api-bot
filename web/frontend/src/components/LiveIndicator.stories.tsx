import type { Meta, StoryObj } from '@storybook/react-vite'
import { LiveIndicator } from './LiveIndicator'
import { Wrapper } from './Wrapper'

const meta = {
  title: 'Data display/LiveIndicator',
  component: LiveIndicator,
} satisfies Meta<typeof LiveIndicator>

export default meta
type Story = StoryObj<typeof meta>

export const States: Story = {
  args: { connectionState: 'connected', lastMessageTime: new Date() },
  render: () => (
    <Wrapper dir="column" gap="md" align="start">
      <LiveIndicator connectionState="connected" lastMessageTime={new Date()} />
      <LiveIndicator connectionState="connecting" lastMessageTime={null} />
      <LiveIndicator connectionState="reconnecting" lastMessageTime={new Date(Date.now() - 60_000)} />
      <LiveIndicator connectionState="disconnected" lastMessageTime={null} />
    </Wrapper>
  ),
}
