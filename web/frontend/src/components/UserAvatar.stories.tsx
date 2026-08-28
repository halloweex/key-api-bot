import type { Meta, StoryObj } from '@storybook/react-vite'
import { UserAvatar } from './UserAvatar'
import { Wrapper } from './Wrapper'

const meta = {
  title: 'Data display/UserAvatar',
  component: UserAvatar,
  args: { name: 'Olena Kovalenko' },
} satisfies Meta<typeof UserAvatar>

export default meta
type Story = StoryObj<typeof meta>

export const Generated: Story = {}

export const Sizes: Story = {
  render: () => (
    <Wrapper dir="row" gap="md" align="center">
      <UserAvatar name="Olena Kovalenko" size={24} />
      <UserAvatar name="Olena Kovalenko" size={32} />
      <UserAvatar name="Olena Kovalenko" size={48} />
    </Wrapper>
  ),
}
