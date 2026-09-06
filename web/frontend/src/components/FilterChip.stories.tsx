import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { FilterChip } from './FilterChip'
import { Wrapper } from './Wrapper'

const meta = {
  title: 'Forms/FilterChip',
  component: FilterChip,
} satisfies Meta<typeof FilterChip>

export default meta
type Story = StoryObj<typeof meta>

function ChipRow({ tone }: { tone?: Parameters<typeof FilterChip>[0]['tone'] }) {
  const [active, setActive] = useState('week')
  const options = ['today', 'week', 'month']
  return (
    <Wrapper dir="row" gap="sm">
      {options.map((o) => (
        <FilterChip key={o} active={active === o} onClick={() => setActive(o)} tone={tone}>
          {o}
        </FilterChip>
      ))}
    </Wrapper>
  )
}

export const Purple: Story = {
  args: { active: true, onClick: () => {}, children: 'chip' },
  render: () => <ChipRow tone="purple" />,
}

export const Slate: Story = {
  args: { active: true, onClick: () => {}, children: 'chip' },
  render: () => <ChipRow tone="slate" />,
}

export const Blue: Story = {
  args: { active: true, onClick: () => {}, children: 'chip' },
  render: () => <ChipRow tone="blue" />,
}

export const Disabled: Story = {
  args: { active: false, onClick: () => {}, children: 'unavailable', disabled: true },
}
