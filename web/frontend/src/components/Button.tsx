import { type ButtonHTMLAttributes, type ReactNode } from 'react'

// ─── Button ──────────────────────────────────────────────────────────────────
//
// Visual look is owned entirely by the component. Consumers express intent via
// `variant` and `size`; layout (full-width inside a flex parent, alignment
// against siblings, etc.) is the parent's job via <Wrapper>.
//
// Behavioural HTML attributes (onClick, disabled, type, aria-*, …) are still
// accepted — only the visual escape hatches className/style are removed.

type ButtonVariant = 'primary' | 'secondary' | 'ghost'
type ButtonSize = 'sm' | 'md' | 'lg' | 'pill'

type NativeProps = Omit<
  ButtonHTMLAttributes<HTMLButtonElement>,
  'className' | 'style' | 'children'
>

interface ButtonProps extends NativeProps {
  children: ReactNode
  variant?: ButtonVariant
  size?: ButtonSize
  /** Stretch to fill its parent's width. Use inside a <Wrapper flex={1}> for split rows. */
  fullWidth?: boolean
}

const variantClass: Record<ButtonVariant, string> = {
  primary:
    'bg-gradient-to-r from-blue-600 to-purple-600 text-white hover:from-blue-700 hover:to-purple-700 active:from-blue-800 active:to-purple-800 shadow-sm hover:shadow',
  secondary:
    'bg-white text-slate-700 hover:bg-slate-50 active:bg-slate-100 border border-slate-200 shadow-sm',
  ghost:
    'bg-transparent text-slate-600 hover:bg-slate-100 hover:text-slate-900',
}

const sizeClass: Record<ButtonSize, string> = {
  sm: 'px-3 py-1.5 text-xs',
  md: 'px-4 py-2 text-sm',
  lg: 'px-5 py-2.5 text-base',
  // `pill` is the responsive filter-chip size used in segmented controls.
  pill: 'px-2 sm:px-3 py-1 sm:py-1.5 text-xs sm:text-sm whitespace-nowrap',
}

export function Button({
  children,
  variant = 'secondary',
  size = 'md',
  fullWidth = false,
  ...rest
}: ButtonProps) {
  return (
    <button
      {...rest}
      className={`inline-flex items-center justify-center gap-1 font-medium rounded-lg transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-purple-500/40 focus:ring-offset-1 disabled:opacity-50 disabled:cursor-not-allowed disabled:shadow-none ${variantClass[variant]} ${sizeClass[size]} ${fullWidth ? 'w-full' : ''}`}
    >
      {children}
    </button>
  )
}
