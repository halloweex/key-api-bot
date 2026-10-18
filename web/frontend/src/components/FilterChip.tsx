import { memo, type ReactNode } from 'react'

// ─── FilterChip ──────────────────────────────────────────────────────────────
//
// Toggleable pill used in segmented controls / source filters. Visual is owned
// here — consumers pass `active` state, an optional `tone`, and the label
// (children). No className/style escape.

type Tone = 'purple' | 'slate' | 'blue'
/** `md` is a thumb-sized chip (36px) for forms used on a phone. */
type Size = 'sm' | 'md'

interface FilterChipProps {
  active: boolean
  onClick: () => void
  children: ReactNode
  tone?: Tone
  size?: Size
  disabled?: boolean
}

const activeClass: Record<Tone, string> = {
  purple: 'bg-purple-100 text-purple-700',
  slate: 'bg-slate-700 text-white',
  blue: 'bg-blue-100 text-blue-700',
}

const sizeClass: Record<Size, string> = {
  sm: 'px-2.5 py-1 text-xs',
  md: 'px-3 py-1.5 text-sm min-h-[36px]',
}

export const FilterChip = memo(function FilterChip({
  active,
  onClick,
  children,
  tone = 'purple',
  size = 'sm',
  disabled = false,
}: FilterChipProps) {
  const state = active ? activeClass[tone] : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      aria-pressed={active}
      className={`${sizeClass[size]} rounded-lg font-medium tabular-nums transition-colors disabled:opacity-50 disabled:cursor-not-allowed ${state}`}
    >
      {children}
    </button>
  )
})
