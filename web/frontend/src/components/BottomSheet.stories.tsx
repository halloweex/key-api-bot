import { useState, type ReactNode } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { BottomSheet } from './BottomSheet'
import { Button } from './Button'
import { NumberWithUnit } from './NumberWithUnit'
import { SegmentedControl } from './SegmentedControl'
import { Wrapper } from './Wrapper'

/**
 * The phone's way of asking a question without leaving the page: a panel that
 * rises from the bottom, under the thumb, over a dimmed page. Its desktop
 * counterpart is `Popover`.
 *
 * One level only — a sheet never opens another sheet. It closes on ×, on a
 * tap on the dimmed page, or on Escape; the footer holds the one action that
 * ends the task, sticky above the home indicator however long the body is.
 *
 * Every story opens with the sheet up. Close it and the button behind brings
 * it back.
 */

function Live({
  title, footer = true, initialOpen = true, children,
}: {
  title: string
  footer?: boolean
  initialOpen?: boolean
  children: ReactNode
}) {
  const [open, setOpen] = useState(initialOpen)
  return (
    <Wrapper dir="column" gap="md" padding="md">
      <p className="text-sm text-slate-600">
        The page behind the sheet. It does not scroll while the sheet is up.
      </p>
      <div>
        <Button variant="secondary" onClick={() => setOpen(true)}>Open the sheet</Button>
      </div>
      <BottomSheet
        open={open}
        title={title}
        onClose={() => setOpen(false)}
        closeLabel="Close"
        footer={footer
          ? <Button fullWidth variant="primary" onClick={() => setOpen(false)}>Done</Button>
          : undefined}
      >
        {children}
      </BottomSheet>
    </Wrapper>
  )
}

function OrdersBody() {
  const [count, setCount] = useState<number | null>(2)
  const [period, setPeriod] = useState<'all' | 'last' | 'range'>('all')
  return (
    <Wrapper dir="column" gap="md">
      <Wrapper dir="column" gap="xs">
        <span className="text-xs font-medium text-slate-500">How many</span>
        <NumberWithUnit
          ariaLabel="Number of orders"
          stepper
          min={1}
          max={1000}
          value={count}
          onChange={setCount}
          unit={count === 1 ? 'order' : 'orders'}
        />
      </Wrapper>
      <Wrapper dir="column" gap="xs">
        <span className="text-xs font-medium text-slate-500">When</span>
        <SegmentedControl
          ariaLabel="When"
          size="sm"
          fullWidth
          value={period}
          onChange={setPeriod}
          options={[
            { value: 'all', label: 'All time' },
            { value: 'last', label: 'Last N days' },
            { value: 'range', label: 'From … to …' },
          ]}
        />
      </Wrapper>
    </Wrapper>
  )
}

const meta = {
  title: 'Overlays/BottomSheet',
  component: BottomSheet,
  parameters: { layout: 'fullscreen' },
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  // Required props; every story renders through `Live`.
  args: {
    open: true,
    title: 'Placed',
    onClose: () => {},
    children: null,
  },
} satisfies Meta<typeof BottomSheet>

export default meta
type Story = StoryObj<typeof meta>

/** A condition's editor, as a phone shows it, with «Done» in the footer. */
export const WithFooter: Story = {
  render: () => (
    <Live title="Placed">
      <OrdersBody />
    </Live>
  ),
}

/**
 * A body longer than the screen: it scrolls inside the sheet (and does not
 * hand the scroll to the page at its end), while the footer stays put.
 */
export const LongBody: Story = {
  render: () => (
    <Live title="Ordered via">
      <Wrapper dir="column" gap="sm">
        {Array.from({ length: 30 }, (_, i) => (
          <p key={i} className="text-sm text-slate-700">Line {i + 1} of a long list</p>
        ))}
      </Wrapper>
    </Live>
  ),
}

/**
 * A menu: no footer, because picking an item is the action — the sheet
 * closes and the next one opens in its place.
 */
export const MenuWithoutFooter: Story = {
  render: () => (
    <Live title="Add a condition" footer={false}>
      <Wrapper dir="column" gap="xs">
        {['Number of orders', 'Last order', 'First order', 'Total spent'].map((label) => (
          <Button key={label} variant="ghost" fullWidth>{label}</Button>
        ))}
      </Wrapper>
    </Live>
  ),
}
