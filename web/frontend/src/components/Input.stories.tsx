import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { Input } from './Input'
import { Wrapper } from './Wrapper'

const meta = {
  title: 'Forms/Input',
  component: Input,
} satisfies Meta<typeof Input>

export default meta
type Story = StoryObj<typeof meta>

function Controlled(props: Omit<Parameters<typeof Input>[0], 'value' | 'onChange'>) {
  const [value, setValue] = useState('')
  return <Input {...props} value={value} onChange={setValue} />
}

export const Default: Story = {
  args: { value: '', onChange: () => {} },
  render: () => <Controlled placeholder="Type here…" />,
}

export const Sizes: Story = {
  args: { value: '', onChange: () => {} },
  render: () => (
    <Wrapper dir="column" gap="md" align="start">
      <Controlled size="xs" placeholder="xs" />
      <Controlled size="sm" placeholder="sm" />
      <Controlled size="md" placeholder="md" />
    </Wrapper>
  ),
}

export const Widths: Story = {
  args: { value: '', onChange: () => {} },
  render: () => (
    <Wrapper dir="column" gap="md">
      <Controlled width="narrow" placeholder="narrow" />
      <Controlled width="search" type="search" placeholder="search width" />
      <Controlled width="wide" placeholder="wide" />
      <Controlled width="full" placeholder="full" />
    </Wrapper>
  ),
}

export const WithPrefix: Story = {
  args: { value: '', onChange: () => {} },
  render: () => <Controlled type="number" prefix="₴" placeholder="0.00" width="narrow" />,
}
