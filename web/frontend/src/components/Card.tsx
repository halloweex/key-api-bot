import { type ReactNode } from 'react'

// ─── Card ────────────────────────────────────────────────────────────────────
//
// Visual frame primitive. All surface styling lives here — consumers express
// intent via semantic props only. Internal layout of children is the
// consumer's job (see <Wrapper>).

type CardVariant = 'default' | 'dark'
type CardAnimate = 'none' | 'chart-in'

interface CardProps {
  children: ReactNode
  /** Surface tone. `dark` = translucent slate tile, used for nested metrics on darker backgrounds. */
  variant?: CardVariant
  /** Entry animation when the card mounts. */
  animate?: CardAnimate
}

const variantClass: Record<CardVariant, string> = {
  default: 'bg-white border-slate-200/60 shadow-[var(--shadow-card)]',
  dark: 'bg-slate-700/50 border-slate-600/40 shadow-[var(--shadow-card)]',
}

const animateClass: Record<CardAnimate, string> = {
  none: '',
  'chart-in': 'animate-chart-in',
}

export function Card({ children, variant = 'default', animate = 'none' }: CardProps) {
  return (
    <div className={`rounded-xl border ${variantClass[variant]} ${animateClass[animate]}`}>
      {children}
    </div>
  )
}

// ─── CardHeader ──────────────────────────────────────────────────────────────
//
// Pure frame: padding + bottom divider. Children are laid out by the consumer
// — typically with <Wrapper dir="row" justify="between" align="center"> for
// title/actions arrangements.

interface CardHeaderProps {
  children: ReactNode
}

export function CardHeader({ children }: CardHeaderProps) {
  return (
    <div className="px-3 sm:px-5 py-3 sm:py-4 border-b border-slate-100">
      {children}
    </div>
  )
}

// ─── CardTitle ───────────────────────────────────────────────────────────────

interface CardTitleProps {
  children: ReactNode
}

export function CardTitle({ children }: CardTitleProps) {
  return (
    <h3 className="text-base font-semibold text-slate-800 tracking-tight">
      {children}
    </h3>
  )
}

// ─── CardContent ─────────────────────────────────────────────────────────────
//
// Inner surface frame.
//   padding="default"  — standard responsive padding (p-3 sm:p-5)
//   padding="compact"  — tight padding for dense tiles (py-2 px-3)
//   padding="none"     — flush content (e.g. embedded list/table that owns its own padding)
//   padding="table"    — flush on mobile, side+bottom padding on desktop. Used when a
//                        scrolling table fills the card on mobile but should breathe inside
//                        a desktop card.

type CardContentPadding = 'default' | 'compact' | 'none' | 'table'

interface CardContentProps {
  children: ReactNode
  padding?: CardContentPadding
}

const contentPaddingClass: Record<CardContentPadding, string> = {
  default: 'p-3 sm:p-5',
  compact: 'py-2 px-3',
  none: '',
  table: 'p-0 sm:px-5 sm:pb-5',
}

export function CardContent({ children, padding = 'default' }: CardContentProps) {
  return <div className={contentPaddingClass[padding]}>{children}</div>
}
