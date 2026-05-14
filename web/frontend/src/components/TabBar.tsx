import { memo, type ReactNode } from 'react'

// ─── TabBar + TabButton ──────────────────────────────────────────────────────
//
// Generic segmented control. Two visual variants:
//
//   filled    — slate-100 pill background, active tab is a white card with
//               subtle shadow. Used for top-level page tabs (Reports).
//   bordered  — bare buttons with light-blue active state and border. Used
//               inside cards for sub-section switching (cohort analysis).
//
// Visual + active-state colouring is owned here. Consumers pass `active`,
// `onClick`, and the label as children. No className/style escape.

type Variant = 'filled' | 'bordered'

interface TabBarProps {
  variant?: Variant
  children: ReactNode
  ariaLabel?: string
}

const barClass: Record<Variant, string> = {
  filled: 'flex gap-1 bg-slate-100 p-1 rounded-xl w-fit',
  bordered: 'flex flex-wrap gap-2',
}

export const TabBar = memo(function TabBar({ variant = 'filled', children, ariaLabel }: TabBarProps) {
  return (
    <div role="tablist" aria-label={ariaLabel} className={barClass[variant]}>
      {children}
    </div>
  )
})

interface TabButtonProps {
  active: boolean
  onClick: () => void
  children: ReactNode
  variant?: Variant
}

const filledTab: Record<'active' | 'inactive', string> = {
  active: 'bg-white text-slate-900 shadow-sm',
  inactive: 'text-slate-500 hover:text-slate-700',
}

const borderedTab: Record<'active' | 'inactive', string> = {
  active: 'bg-blue-100 text-blue-700 border border-blue-200',
  inactive: 'text-slate-600 hover:bg-slate-100 border border-transparent',
}

const tabBase: Record<Variant, string> = {
  filled: 'px-4 py-2 rounded-lg text-sm font-medium transition-all',
  bordered: 'px-3 py-2 text-sm font-medium rounded-lg transition-colors whitespace-nowrap',
}

export const TabButton = memo(function TabButton({
  active,
  onClick,
  children,
  variant = 'filled',
}: TabButtonProps) {
  const state = active
    ? (variant === 'filled' ? filledTab.active : borderedTab.active)
    : (variant === 'filled' ? filledTab.inactive : borderedTab.inactive)
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      onClick={onClick}
      className={`${tabBase[variant]} ${state}`}
    >
      {children}
    </button>
  )
})
