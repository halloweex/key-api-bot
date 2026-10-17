import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { ChevronDown } from 'lucide-react'
import { Button } from './Button'
import { Popover } from './Popover'
import { Wrapper } from './Wrapper'

/**
 * `BottomSheet`'s desktop counterpart: the same content, anchored under the
 * control that opened it. A light panel of choices, not the dark help tooltip
 * `InfoPopover` is.
 *
 * The caller renders the trigger and owns `open`; the popover places itself
 * and closes on a click outside it or on Escape. It sits in the flow of its
 * wrapper, so it scrolls with the page.
 */

const PAST = [
  { name: 'ks_oct_repeat', line: '4 101 recipients · Oct 1, 2026' },
  { name: 'ks_aug_vip', line: '1 067 recipients · Aug 20, 2026' },
  { name: 'ks_jul_lost', line: '2 380 recipients · Jul 3, 2026' },
]

function Live({ initialOpen = true, align }: { initialOpen?: boolean; align?: 'start' | 'end' }) {
  const [open, setOpen] = useState(initialOpen)
  const [picked, setPicked] = useState<string | null>(null)
  return (
    <Wrapper dir="column" gap="sm">
      <Wrapper dir="row" justify={align === 'end' ? 'end' : 'start'}>
        <Popover
          open={open}
          onClose={() => setOpen(false)}
          align={align}
          label="Start from a past campaign"
          trigger={
            <Button variant="secondary" size="sm" aria-haspopup="dialog" aria-expanded={open}
                    onClick={() => setOpen(!open)}>
              Like a past campaign… <ChevronDown className="h-3.5 w-3.5" aria-hidden />
            </Button>
          }
        >
          <ul className="flex flex-col">
            {PAST.map((c) => (
              <li key={c.name}>
                <button
                  type="button"
                  onClick={() => { setPicked(c.name); setOpen(false) }}
                  className="flex w-full flex-col rounded-lg px-2 py-2.5 text-left hover:bg-slate-50"
                >
                  <span className="text-sm font-medium text-slate-800">{c.name}</span>
                  <span className="text-xs text-slate-500">{c.line}</span>
                </button>
              </li>
            ))}
          </ul>
        </Popover>
      </Wrapper>
      <p className="text-xs text-slate-500">
        {picked ? `Picked: ${picked}` : 'Click outside the panel, or press Escape, to close it.'}
      </p>
    </Wrapper>
  )
}

const meta = {
  title: 'Overlays/Popover',
  component: Popover,
  // Required props; every story renders through `Live`.
  args: {
    open: true,
    onClose: () => {},
    trigger: null,
    label: 'Popover',
    children: null,
  },
} satisfies Meta<typeof Popover>

export default meta
type Story = StoryObj<typeof meta>

/** Open, lined up with the trigger's start edge. */
export const Open: Story = {
  render: () => <Live />,
}

/** For a trigger at the right edge of a row: the panel lines up with its end. */
export const AlignedToTheEnd: Story = {
  render: () => <Live align="end" />,
}

/** Closed, as it first renders on the page. */
export const Closed: Story = {
  render: () => <Live initialOpen={false} />,
}
