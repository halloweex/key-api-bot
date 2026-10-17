import { useRef, type KeyboardEvent } from 'react'

// ─── SegmentedControl ────────────────────────────────────────────────────────
//
// A handful of mutually exclusive choices, all visible, one selected — the
// iOS / Material segmented button. It replaces a select wherever there are
// two to five short options, because a select hides the alternatives behind a
// tap and the whole point of these is seeing them side by side ("All time |
// Last N days | From … to …").
//
// A radio group to assistive technology, with the arrow keys moving the
// choice and one tab stop for the whole control (WAI-ARIA radio group).
// Visuals are owned here; consumers say `size` and whether it stretches.

export interface SegmentOption<T extends string> {
  value: T
  label: string
  disabled?: boolean
}

type Size = 'sm' | 'md'

const sizeClass: Record<Size, string> = {
  // 36px on a phone, the floor for a thumb next to other targets.
  sm: 'px-2.5 py-1.5 text-xs min-h-[36px] sm:min-h-0',
  md: 'px-3 py-2 text-sm min-h-[40px] sm:min-h-0',
}

export function SegmentedControl<T extends string>({
  options,
  value,
  onChange,
  ariaLabel,
  size = 'md',
  fullWidth = false,
}: {
  options: Array<SegmentOption<T>>
  value: T
  onChange: (value: T) => void
  ariaLabel: string
  size?: Size
  /** Share the width equally — the phone layout. */
  fullWidth?: boolean
}) {
  const refs = useRef<Array<HTMLButtonElement | null>>([])
  const enabled = options.filter((o) => !o.disabled)
  // One tab stop always: the chosen segment, or — when the value matches none
  // of them — the first that can be chosen, or the control is unreachable by
  // keyboard.
  const tabStop = options.some((o) => o.value === value) ? value : enabled[0]?.value

  function move(e: KeyboardEvent, from: number) {
    const delta = e.key === 'ArrowRight' || e.key === 'ArrowDown' ? 1
      : e.key === 'ArrowLeft' || e.key === 'ArrowUp' ? -1 : 0
    if (!delta || !enabled.length) return
    e.preventDefault()
    const at = enabled.findIndex((o) => o.value === options[from].value)
    const next = enabled[(at + delta + enabled.length) % enabled.length]
    onChange(next.value)
    refs.current[options.indexOf(next)]?.focus()
  }

  return (
    <div
      role="radiogroup"
      aria-label={ariaLabel}
      className={`${fullWidth ? 'flex w-full' : 'inline-flex max-w-full flex-wrap'} gap-0.5
                  rounded-lg bg-slate-100 p-0.5`}
    >
      {options.map((o, i) => {
        const active = o.value === value
        return (
          <button
            key={o.value}
            ref={(el) => { refs.current[i] = el }}
            type="button"
            role="radio"
            aria-checked={active}
            disabled={o.disabled}
            tabIndex={o.value === tabStop ? 0 : -1}
            onClick={() => onChange(o.value)}
            onKeyDown={(e) => move(e, i)}
            className={`${sizeClass[size]} ${
              // Stretched segments share the row and wrap their own words; a
              // segment wrapping onto a second row reads as a broken control.
              fullWidth ? 'flex-1 basis-0 text-center leading-tight' : 'whitespace-nowrap'
            } rounded-md font-medium transition-colors focus:outline-none
                        focus-visible:ring-2 focus-visible:ring-purple-500/40
                        disabled:opacity-40 disabled:cursor-not-allowed ${
              active
                ? 'bg-white text-slate-900 shadow-sm'
                : 'text-slate-600 hover:text-slate-900'
            }`}
          >
            {o.label}
          </button>
        )
      })}
    </div>
  )
}
