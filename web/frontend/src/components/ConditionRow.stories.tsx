import { useState, type ReactNode } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { CalendarClock, ShoppingBag, Tag } from 'lucide-react'
import { Button } from './Button'
import { ConditionRow } from './ConditionRow'
import { NumberWithUnit } from './NumberWithUnit'
import { SegmentedControl } from './SegmentedControl'
import { Wrapper } from './Wrapper'
import type { IconComponent } from './icons'

/**
 * One condition, said as one sentence: «Placed [2+ orders] [from Jan 1 to
 * Oct 14, 2026]». The fixed words are plain text; the parts somebody chose
 * are blanks — dotted underline, and each opens the editor — so what is set
 * and what can be changed tell apart without a single label.
 *
 * The row does not know what it describes: the caller passes the sentence and
 * the editor. On desktop the editor is drawn inline under the sentence while
 * `editing`; on a phone the caller puts it in a bottom sheet and passes
 * nothing. What is wrong with the condition is said under the sentence in
 * both cases, so it stays on screen while the sheet is closed.
 */

function Live({
  icon, label, blanks, problem, hint, initialEditing = false, editor,
}: {
  icon: IconComponent
  label: string
  blanks: string[]
  problem?: string | null
  hint?: ReactNode
  initialEditing?: boolean
  editor?: ReactNode
}) {
  const [editing, setEditing] = useState(initialEditing)
  const [removed, setRemoved] = useState(false)
  if (removed) {
    return (
      <Button variant="ghost" size="sm" onClick={() => setRemoved(false)}>
        Removed — bring it back
      </Button>
    )
  }
  return (
    <ConditionRow
      icon={icon}
      label={label}
      blanks={blanks}
      editing={editing}
      onEdit={() => setEditing(!editing)}
      onDone={() => setEditing(false)}
      onRemove={() => setRemoved(true)}
      removeLabel={`Remove condition: ${label}`}
      editLabel={(b) => `Change: ${b}`}
      doneLabel="Done"
      problem={problem}
      hint={hint}
    >
      {editor}
    </ConditionRow>
  )
}

/** A small editor, enough to show where the inline one goes. */
function OrdersEditor() {
  const [count, setCount] = useState<number | null>(2)
  const [period, setPeriod] = useState<'all' | 'last' | 'range'>('range')
  return (
    <Wrapper dir="column" gap="md">
      <NumberWithUnit
        ariaLabel="Number of orders"
        stepper
        min={1}
        max={1000}
        value={count}
        onChange={setCount}
        unit={count === 1 ? 'order' : 'orders'}
      />
      <SegmentedControl
        ariaLabel="When"
        size="sm"
        value={period}
        onChange={setPeriod}
        options={[
          { value: 'all', label: 'All time' },
          { value: 'last', label: 'Last N days' },
          { value: 'range', label: 'From … to …' },
        ]}
      />
    </Wrapper>
  )
}

const meta = {
  title: 'Audience builder/ConditionRow',
  component: ConditionRow,
  parameters: { layout: 'padded' },
  // Required props; every story renders through `Live`.
  args: {
    icon: ShoppingBag,
    label: 'Placed',
    blanks: [],
    editing: false,
    onEdit: () => {},
    onDone: () => {},
    onRemove: () => {},
    removeLabel: 'Remove',
    editLabel: (b: string) => b,
    doneLabel: 'Done',
  },
} satisfies Meta<typeof ConditionRow>

export default meta
type Story = StoryObj<typeof meta>

/**
 * Read state: the sentence and its blanks. Tapping a blank opens the editor
 * (here, toggles `editing`).
 */
export const Reading: Story = {
  render: () => (
    <Live
      icon={ShoppingBag}
      label="Placed"
      blanks={['2+ orders', 'from Jan 1 to Oct 14, 2026']}
      editor={<OrdersEditor />}
    />
  ),
}

/** Editing on desktop: the inputs open under the sentence, with «Done» at the end. */
export const EditingInline: Story = {
  render: () => (
    <Live
      icon={ShoppingBag}
      label="Placed"
      blanks={['2+ orders', 'from Jan 1 to Oct 14, 2026']}
      initialEditing
      editor={<OrdersEditor />}
    />
  ),
}

/**
 * An unfinished condition: «bought …» with nothing picked. The row is marked
 * and says it is not counted — a campaign must never be frozen with a
 * condition the person believes is applied and is not.
 */
export const WithProblem: Story = {
  render: () => (
    <Live
      icon={Tag}
      label="Bought"
      blanks={['… pick a brand, category or code', 'over all time']}
      problem="Not filled in — not counted yet."
    />
  ),
}

/** A reversed range is the other problem a row says out loud. */
export const WithInvertedRange: Story = {
  render: () => (
    <Live
      icon={CalendarClock}
      label="Last order"
      blanks={['180 to 90 days ago']}
      problem="The start is after the end — not counted until it is fixed."
    />
  ),
}

/**
 * A hint: the owner's slip on 2026-10-17, 92 typed into an order count. The
 * condition is valid, so it is no `problem` — it is a suspicion, said under
 * the sentence with the one-tap fix beside it.
 */
export const WithHint: Story = {
  render: () => (
    <Live
      icon={ShoppingBag}
      label="Placed"
      blanks={['92+ orders', 'from Jan 1 to Oct 14, 2026']}
      hint={
        <Wrapper dir="row" gap="sm" align="center" wrap>
          <span className="text-xs text-amber-800">
            Nobody has 92 orders. Did you mean 92 days?
          </span>
          <Button size="sm" variant="secondary">It&apos;s 92 days</Button>
        </Wrapper>
      }
    />
  ),
}
