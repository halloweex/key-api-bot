import { memo, type ReactNode } from 'react'

// ─── Badge ───────────────────────────────────────────────────────────────────
//
// Compact coloured pill / tag used for status, category, role, count, etc.
// Visual is owned entirely by this component — consumers express intent via
// `tone` and `shape` only. There are no className/style escapes.
//
// Shapes:
//   pill   — fully rounded (rounded-full), default
//   tag    — softly rounded (rounded-md), e.g. role labels in dropdowns
//   square — minimally rounded (rounded), e.g. tight inline tags

type BadgeTone =
  | 'neutral'
  | 'green'
  | 'red'
  | 'blue'
  | 'purple'
  | 'orange'
  | 'yellow'
  | 'cyan'
  | 'indigo'
  | 'rose'
  | 'teal'

type BadgeShape = 'pill' | 'tag' | 'square'

interface BadgeProps {
  children: ReactNode
  tone?: BadgeTone
  shape?: BadgeShape
  /** Optional icon shown to the left of the label. */
  icon?: ReactNode
}

const toneClass: Record<BadgeTone, string> = {
  neutral: 'bg-slate-100 text-slate-600',
  green: 'bg-green-100 text-green-700',
  red: 'bg-red-100 text-red-700',
  blue: 'bg-blue-100 text-blue-700',
  purple: 'bg-purple-100 text-purple-700',
  orange: 'bg-amber-100 text-amber-700',
  yellow: 'bg-yellow-100 text-yellow-700',
  cyan: 'bg-cyan-100 text-cyan-700',
  indigo: 'bg-indigo-100 text-indigo-700',
  rose: 'bg-rose-100 text-rose-700',
  teal: 'bg-teal-100 text-teal-700',
}

const shapeClass: Record<BadgeShape, string> = {
  pill: 'rounded-full',
  tag: 'rounded-md',
  square: 'rounded',
}

export const Badge = memo(function Badge({
  children,
  tone = 'neutral',
  shape = 'pill',
  icon,
}: BadgeProps) {
  const padding = icon ? 'px-2.5 py-1 gap-1' : 'px-2 py-0.5'
  return (
    <span
      className={`inline-flex items-center ${padding} text-xs font-medium ${shapeClass[shape]} ${toneClass[tone]}`}
    >
      {icon}
      {children}
    </span>
  )
})
