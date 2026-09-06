import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { Select } from './Select'
import { Wrapper } from './Wrapper'

const OPTIONS = [
  { value: 'instagram', label: 'Instagram' },
  { value: 'telegram', label: 'Telegram' },
  { value: 'shopify', label: 'Shopify' },
]

const meta = {
  title: 'Forms/Select',
  component: Select,
} satisfies Meta<typeof Select>

export default meta
type Story = StoryObj<typeof meta>

function Controlled(props: Omit<Parameters<typeof Select>[0], 'value' | 'onChange'>) {
  const [value, setValue] = useState<string | null>(null)
  return <Select {...props} value={value} onChange={setValue} />
}

export const Framed: Story = {
  args: { options: OPTIONS, value: null, onChange: () => {} },
  render: () => <Controlled options={OPTIONS} emptyLabel="All sources" />,
}

export const Variants: Story = {
  args: { options: OPTIONS, value: null, onChange: () => {} },
  render: () => (
    <Wrapper dir="row" gap="md" align="center">
      <Controlled options={OPTIONS} variant="framed" emptyLabel="framed" />
      <Controlled options={OPTIONS} variant="compact" emptyLabel="compact" />
      <Controlled options={OPTIONS} variant="pill" emptyLabel="pill" />
    </Wrapper>
  ),
}

export const WithPlaceholder: Story = {
  args: { options: OPTIONS, value: null, onChange: () => {} },
  render: () => <Controlled options={OPTIONS} placeholder="Add a brand…" />,
}
