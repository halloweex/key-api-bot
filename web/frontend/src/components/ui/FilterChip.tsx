import { memo, type ReactNode } from 'react'

// ─── FilterChip ──────────────────────────────────────────────────────────────
//
// Toggleable pill used in segmented controls / source filters. Visual is owned
// here — consumers pass `active` state, an optional `tone`, and the label
// (children). No className/style escape.

type Tone = 'purple' | 'slate' | 'blue'

interface FilterChipProps {
  active: boolean
  onClick: () => void
  children: ReactNode
  tone?: Tone
  disabled?: boolean
}

const activeClass: Record<Tone, string> = {
  purple: 'bg-purple-100 text-purple-700',
  slate: 'bg-slate-700 text-white',
  blue: 'bg-blue-100 text-blue-700',
}

export const FilterChip = memo(function FilterChip({
  active,
  onClick,
  children,
  tone = 'purple',
  disabled = false,
}: FilterChipProps) {
  const state = active ? activeClass[tone] : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className={`px-2.5 py-1 rounded-lg text-xs font-medium transition-colors disabled:opacity-50 disabled:cursor-not-allowed ${state}`}
    >
      {children}
    </button>
  )
})
