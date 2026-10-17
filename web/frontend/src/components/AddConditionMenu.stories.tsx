import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { userEvent, within } from 'storybook/test'
import {
  Banknote, CalendarClock, MapPin, Receipt, ShoppingBag, Store, Tag, UserPlus, VenusAndMars,
} from 'lucide-react'
import { AddConditionMenu, type ConditionMenuFamily } from './AddConditionMenu'
import { Wrapper } from './Wrapper'

/**
 * «+ Condition»: every condition a group can have, by the question it
 * answers, each with an example line. A popover on desktop, a bottom sheet on
 * a phone. Picking closes the menu at once; the caller inserts the condition
 * and opens it for editing — on a phone that is the next sheet, in place of
 * this one, never on top of it.
 *
 * An item the group cannot take again stays in the list, disabled, with its
 * reason in place of the example: a menu whose contents change from group to
 * group reads as broken.
 *
 * The stories open the menu with a click on the trigger.
 */

const families = (taken: string[] = []): ConditionMenuFamily[] => {
  const item = (key: string, icon: ConditionMenuFamily['items'][number]['icon'],
                label: string, example: string) => ({
    key, icon, label, example,
    disabled: taken.includes(key),
    disabledNote: key === 'orders'
      ? 'This group already counts orders over all time and over a period.'
      : 'Already in this group — change it there.',
  })
  return [
    {
      title: 'Purchase history',
      items: [
        item('orders', ShoppingBag, 'Number of orders',
             '2+ orders from Jan 1 to Oct 14, or 1+ over all time'),
        item('lastOrder', CalendarClock, 'Last order', 'in the last 90 days, or 60+ days ago'),
        item('firstOrder', UserPlus, 'First order', 'in the last 30 days — new customers'),
        item('spent', Banknote, 'Total spent', 'from ₴5,000 over all their orders'),
        item('aov', Receipt, 'Average order', 'from ₴1,500 per order'),
      ],
    },
    {
      title: 'What they bought',
      items: [
        item('bought', Tag, 'Bought a brand, category or code', 'Anua, a category, or code KS-AUG'),
        item('source', Store, 'Ordering channel', 'Instagram, Telegram, the website, the exhibition'),
      ],
    },
    {
      title: 'Who they are',
      items: [
        item('city', MapPin, 'City', 'Kyiv, Lviv'),
        item('gender', VenusAndMars, 'Gender', 'women only, or men only'),
      ],
    },
  ]
}

function Live({ phone, taken }: { phone: boolean; taken?: string[] }) {
  const [picked, setPicked] = useState<string | null>(null)
  return (
    <Wrapper dir="column" gap="sm" align="start">
      <AddConditionMenu
        families={families(taken)}
        onPick={setPicked}
        triggerLabel="Condition"
        menuTitle="Add a condition"
        phone={phone}
        closeLabel="Close"
      />
      <span className="text-xs text-slate-500">
        {picked ? `Picked: ${picked}` : 'Nothing picked yet.'}
      </span>
    </Wrapper>
  )
}

// `userEvent` imported rather than taken from the play context, so the same
// play runs under portable stories (composeStories in a test) as well.
const openMenu: Story['play'] = async ({ canvasElement }) => {
  await userEvent.click(within(canvasElement).getByRole('button', { name: /condition/i }))
}

const meta = {
  title: 'Audience builder/AddConditionMenu',
  component: AddConditionMenu,
  // Required props; every story renders through `Live`.
  args: {
    families: families(),
    onPick: () => {},
    triggerLabel: 'Condition',
    menuTitle: 'Add a condition',
    phone: false,
    closeLabel: 'Close',
  },
} satisfies Meta<typeof AddConditionMenu>

export default meta
type Story = StoryObj<typeof meta>

/** Desktop: a popover under the button. */
export const DesktopPopover: Story = {
  render: () => <Live phone={false} />,
  play: openMenu,
}

/**
 * A group that already holds an orders count of each kind, a last order and
 * a city: those stay listed, disabled, and say why.
 */
export const WithTakenItems: Story = {
  render: () => <Live phone={false} taken={['orders', 'lastOrder', 'city']} />,
  play: openMenu,
}

/** A phone: the same list in a bottom sheet. */
export const PhoneSheet: Story = {
  parameters: { layout: 'fullscreen' },
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  render: () => (
    <Wrapper padding="md">
      <Live phone />
    </Wrapper>
  ),
  play: openMenu,
}
