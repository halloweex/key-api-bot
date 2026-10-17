import { useCallback, useState } from 'react'
import { Plus } from 'lucide-react'
import { Button } from './Button'
import { BottomSheet } from './BottomSheet'
import { Popover } from './Popover'
import type { IconComponent } from './icons'

// ─── AddConditionMenu ────────────────────────────────────────────────────────
//
// «+ Условие»: every condition a group can have, by the question it answers,
// each with an example line under it. A group starts empty and grows by
// picking from here — the alternative, every field of every condition on the
// screen at once, is what the owner got lost in.
//
// A popover under the button on desktop, a bottom sheet on a phone. Picking
// closes the menu at once: the caller inserts the condition and opens it for
// editing, which on a phone is the next sheet — in place of this one, never
// on top of it.
//
// An item that cannot be added (the group already has one) stays in the list,
// disabled with its reason, rather than vanishing — a menu whose contents
// change from group to group reads as broken.

export interface ConditionMenuItem {
  key: string
  icon: IconComponent
  label: string
  example: string
  disabled?: boolean
  /** Why it is disabled, said in place of the example. */
  disabledNote?: string
}

export interface ConditionMenuFamily {
  title: string
  items: ConditionMenuItem[]
}

export function AddConditionMenu({
  families,
  onPick,
  triggerLabel,
  menuTitle,
  phone,
  closeLabel,
}: {
  families: ConditionMenuFamily[]
  onPick: (key: string) => void
  triggerLabel: string
  menuTitle: string
  /** A bottom sheet instead of a popover. */
  phone: boolean
  closeLabel: string
}) {
  const [open, setOpen] = useState(false)
  const close = useCallback(() => setOpen(false), [])

  function pick(key: string) {
    setOpen(false)
    onPick(key)
  }

  const list = (
    <div className="flex flex-col gap-3">
      {families.map((family) => (
        <section key={family.title}>
          <h3 className="px-2 pb-1 text-[11px] font-semibold uppercase tracking-wide text-slate-500">
            {family.title}
          </h3>
          <ul>
            {family.items.map(({ key, icon: Icon, label, example, disabled, disabledNote }) => (
              <li key={key}>
                <button
                  type="button"
                  disabled={disabled}
                  onClick={() => pick(key)}
                  className="flex w-full items-start gap-3 rounded-lg px-2 py-2.5 text-left
                             hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50
                             focus:outline-none focus-visible:ring-2 focus-visible:ring-purple-500/40"
                >
                  <span className="mt-0.5 text-slate-400"><Icon className="h-4 w-4" aria-hidden /></span>
                  <span className="min-w-0">
                    <span className="block text-sm font-medium text-slate-800">{label}</span>
                    <span className="block text-xs text-slate-500">
                      {disabled && disabledNote ? disabledNote : example}
                    </span>
                  </span>
                </button>
              </li>
            ))}
          </ul>
        </section>
      ))}
    </div>
  )

  const trigger = (
    <Button
      variant="ghost"
      size="sm"
      aria-haspopup="dialog"
      aria-expanded={open}
      onClick={() => setOpen(!open)}
    >
      <Plus className="h-4 w-4" aria-hidden /> {triggerLabel}
    </Button>
  )

  if (phone) {
    return (
      <>
        {trigger}
        <BottomSheet open={open} title={menuTitle} onClose={close} closeLabel={closeLabel}>
          {list}
        </BottomSheet>
      </>
    )
  }

  return (
    <Popover open={open} onClose={close} trigger={trigger} label={menuTitle}>
      <div className="max-h-[70vh] overflow-y-auto p-1">{list}</div>
    </Popover>
  )
}
