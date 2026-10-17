import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { CalendarCheck, ShoppingBag, Tag } from 'lucide-react'
import {
  SmsChoiceChip, SmsFilterFamily, SmsFilterField, SmsMultiPicker, SmsNumberRange,
} from './SmsAudienceControls'

/**
 * The pieces an SMS audience is built from, shared by every group and by the
 * campaign-wide controls: a family of filters under the question it answers,
 * one field with the example that says what goes in it, a from–to pair, a
 * picker of removable chips, and a choice chip.
 */

function Family() {
  const [from, setFrom] = useState<number | undefined>(2)
  const [to, setTo] = useState<number | undefined>()
  const [brands, setBrands] = useState<string[]>(['Anua'])
  const [on, setOn] = useState(true)
  return (
    <SmsFilterFamily
      icon={ShoppingBag}
      title="How much they bought"
      hint="Counted over their whole history, not over the filtered part of it."
    >
      <SmsFilterField icon={ShoppingBag} label="Orders"
                      example="From 2 — repeat buyers. 1 to 1 — bought exactly once.">
        <SmsNumberRange label="Orders" unit="orders" fromPlaceholder="2"
                        toPlaceholder="10" from={from} to={to}
                        onFrom={setFrom} onTo={setTo} />
      </SmsFilterField>
      <SmsFilterField icon={Tag} label="Bought brand" example="Anua — anyone who ever bought it.">
        <SmsMultiPicker
          label="Bought brand"
          options={[{ value: 'Anua', label: 'Anua' }, { value: 'COSRX', label: 'COSRX' }]}
          values={brands} onChange={setBrands} placeholder="Add a brand"
        />
      </SmsFilterField>
      <SmsFilterField icon={CalendarCheck} label="Level">
        <SmsChoiceChip active={on} onClick={() => setOn(!on)}>VIP · 1 067</SmsChoiceChip>
      </SmsFilterField>
    </SmsFilterFamily>
  )
}

const meta = {
  title: 'SMS/SmsAudienceControls',
  component: SmsFilterFamily,
  parameters: { layout: 'padded' },
  args: { icon: ShoppingBag, title: '', hint: '', children: null },
} satisfies Meta<typeof SmsFilterFamily>

export default meta
type Story = StoryObj<typeof meta>

export const AFamilyOfFilters: Story = {
  render: () => <Family />,
}
