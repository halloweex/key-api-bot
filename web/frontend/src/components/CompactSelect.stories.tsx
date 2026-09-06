import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { CompactSelect } from './CompactSelect'

const meta = {
  title: 'Forms/CompactSelect',
  component: CompactSelect,
} satisfies Meta<typeof CompactSelect>

export default meta
type Story = StoryObj<typeof meta>

const DEFAULT_ARGS = {
  label: 'Window',
  value: 270,
  options: [
    { value: 90, label: '90' },
    { value: 180, label: '180' },
    { value: 270, label: '270' },
    { value: 365, label: '365' },
  ],
  suffix: 'days',
  onChange: () => {},
}

function Controlled() {
  const [value, setValue] = useState(DEFAULT_ARGS.value)
  return <CompactSelect {...DEFAULT_ARGS} value={value} onChange={setValue} />
}

export const Default: Story = {
  args: DEFAULT_ARGS,
  render: () => <Controlled />,
}
