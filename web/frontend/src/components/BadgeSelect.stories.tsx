import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { BadgeSelect } from './BadgeSelect'
import { Wrapper } from './Wrapper'

const ROLE_OPTIONS = [
  { value: 'admin', label: 'Admin' },
  { value: 'viewer', label: 'Viewer' },
]

const meta = {
  title: 'Forms/BadgeSelect',
  component: BadgeSelect,
} satisfies Meta<typeof BadgeSelect>

export default meta
type Story = StoryObj<typeof meta>

function Controlled({ tone }: { tone: Parameters<typeof BadgeSelect>[0]['tone'] }) {
  const [value, setValue] = useState('viewer')
  return <BadgeSelect options={ROLE_OPTIONS} value={value} onChange={setValue} tone={tone} />
}

export const Tones: Story = {
  args: { options: ROLE_OPTIONS, value: 'viewer', onChange: () => {}, tone: 'purple' },
  render: () => (
    <Wrapper dir="row" gap="md" align="center" wrap>
      <Controlled tone="purple" />
      <Controlled tone="blue" />
      <Controlled tone="slate" />
      <Controlled tone="green" />
      <Controlled tone="yellow" />
      <Controlled tone="red" />
    </Wrapper>
  ),
}
