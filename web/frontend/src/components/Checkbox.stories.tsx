import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { Checkbox } from './Checkbox'
import { Wrapper } from './Wrapper'

const meta = {
  title: 'Forms/Checkbox',
  component: Checkbox,
} satisfies Meta<typeof Checkbox>

export default meta
type Story = StoryObj<typeof meta>

function Controlled(props: Omit<Parameters<typeof Checkbox>[0], 'checked' | 'onChange'>) {
  const [checked, setChecked] = useState(false)
  return <Checkbox {...props} checked={checked} onChange={setChecked} />
}

export const Default: Story = {
  args: { checked: false, onChange: () => {} },
  render: () => <Controlled />,
}

export const Sizes: Story = {
  args: { checked: false, onChange: () => {} },
  render: () => (
    <Wrapper dir="row" gap="md" align="center">
      <Controlled size="sm" />
      <Controlled size="md" />
    </Wrapper>
  ),
}

export const Disabled: Story = {
  args: { checked: true, onChange: () => {}, disabled: true },
}
