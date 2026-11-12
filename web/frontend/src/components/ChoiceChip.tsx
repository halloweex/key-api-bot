import { useState } from 'react'
import { Check, ChevronDown } from 'lucide-react'
import { BottomSheet } from './BottomSheet'
import type { IconComponent } from './icons'

// ─── ChoiceChip ──────────────────────────────────────────────────────────────
//
// A phone's way to pick one of a few: the choice as a chip, the options in a
// bottom sheet with a line each on what it means. A segmented control and its
// note took two rows of the first screen where the chip takes part of one
// (the funnel's comparison, 2026-11-11). The target is 44 px while the chip
// draws 32, so the line it sits in grows by the chip's height and no more.
// The sheet's options are radios, so a screen reader hears one choice of
// several and which one is on.

export interface ChoiceOption<T extends string> {
  value: T
  label: string
  /** What choosing it means, under its label in the sheet. */
  note: string
  disabled?: boolean
}

export function ChoiceChip<T extends string>({
  icon: Icon,
  label,
  ariaLabel,
  sheetTitle,
  closeLabel,
  options,
  value,
  onChange,
}: {
  icon?: IconComponent
  /** The chip's own text: the current choice, short. */
  label: string
  ariaLabel: string
  sheetTitle: string
  closeLabel: string
  options: ChoiceOption<T>[]
  value: T
  onChange: (value: T) => void
}) {
  const [open, setOpen] = useState(false)
  return (
    <>
      <button
        type="button"
        onClick={() => setOpen(true)}
        aria-haspopup="dialog"
        aria-expanded={open}
        aria-label={ariaLabel}
        className="-my-1.5 inline-flex min-h-11 items-center"
      >
        <span className="inline-flex h-8 items-center gap-1.5 rounded-full border border-slate-300 bg-white px-3
                         text-[13px] font-medium text-slate-800 shadow-sm">
          {Icon && <Icon className="h-3.5 w-3.5 text-slate-500" aria-hidden />}
          {label}
          <ChevronDown className="h-3.5 w-3.5 text-slate-500" aria-hidden />
        </span>
      </button>
      <BottomSheet open={open} title={sheetTitle} onClose={() => setOpen(false)} closeLabel={closeLabel}>
        <div role="radiogroup" aria-label={sheetTitle} className="flex flex-col gap-2 pb-2">
          {options.map((option) => {
            const on = option.value === value
            return (
              <button
                key={option.value}
                type="button"
                role="radio"
                aria-checked={on}
                disabled={option.disabled}
                onClick={() => { onChange(option.value); setOpen(false) }}
                className={`flex min-h-11 items-center gap-3 rounded-xl border px-3.5 py-2.5 text-left ${
                  on ? 'border-blue-600 bg-blue-50' : 'border-slate-200'} ${option.disabled ? 'opacity-60' : ''}`}
              >
                <span className="flex flex-1 flex-col">
                  <span className="text-sm font-semibold text-slate-900">{option.label}</span>
                  <span className="text-xs text-slate-600">{option.note}</span>
                </span>
                {on && <Check className="h-5 w-5 shrink-0 text-blue-700" aria-hidden />}
              </button>
            )
          })}
        </div>
      </BottomSheet>
    </>
  )
}
