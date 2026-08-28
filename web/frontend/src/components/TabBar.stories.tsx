import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { TabBar, TabButton } from './TabBar'

const meta = {
  title: 'Forms/TabBar',
  component: TabBar,
} satisfies Meta<typeof TabBar>

export default meta
type Story = StoryObj<typeof meta>

function Demo({ variant }: { variant: 'filled' | 'bordered' }) {
  const [tab, setTab] = useState('summary')
  const tabs = ['summary', 'retention', 'ltv']
  return (
    <TabBar variant={variant} ariaLabel="Demo tabs">
      {tabs.map((id) => (
        <TabButton key={id} active={tab === id} onClick={() => setTab(id)} variant={variant}>
          {id}
        </TabButton>
      ))}
    </TabBar>
  )
}

export const Filled: Story = {
  args: { children: null },
  render: () => <Demo variant="filled" />,
}

export const Bordered: Story = {
  args: { children: null },
  render: () => <Demo variant="bordered" />,
}
