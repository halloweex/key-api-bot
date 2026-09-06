import type { Meta, StoryObj } from '@storybook/react-vite'
import { ChatSendButton } from './ChatSendButton'
import { Wrapper } from './Wrapper'

const meta = {
  title: 'Feedback/ChatSendButton',
  component: ChatSendButton,
  args: { onClick: () => {}, ariaLabel: 'Send message' },
} satisfies Meta<typeof ChatSendButton>

export default meta
type Story = StoryObj<typeof meta>

export const States: Story = {
  render: () => (
    <Wrapper dir="row" gap="md" align="center">
      <ChatSendButton onClick={() => {}} ariaLabel="Send" />
      <ChatSendButton onClick={() => {}} ariaLabel="Sending" loading />
      <ChatSendButton onClick={() => {}} ariaLabel="Disabled" disabled />
    </Wrapper>
  ),
}
