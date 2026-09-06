import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { Textarea } from './Textarea'

const meta = {
  title: 'Forms/Textarea',
  component: Textarea,
} satisfies Meta<typeof Textarea>

export default meta
type Story = StoryObj<typeof meta>

function Controlled(props: Omit<Parameters<typeof Textarea>[0], 'value' | 'onChange'>) {
  const [value, setValue] = useState('')
  return <Textarea {...props} value={value} onChange={setValue} />
}

export const Default: Story = {
  args: { value: '', onChange: () => {} },
  render: () => <Controlled placeholder="Message text…" fullWidth />,
}

export const AutoResize: Story = {
  args: { value: '', onChange: () => {} },
  render: () => (
    <Controlled placeholder="Grows as you type, up to 160px" autoResize maxHeight={160} fullWidth />
  ),
}

export const Small: Story = {
  args: { value: '', onChange: () => {} },
  render: () => <Controlled size="sm" placeholder="Compact textarea" fullWidth />,
}
