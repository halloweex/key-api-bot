import { type InputHTMLAttributes, type ReactNode } from 'react'

// ─── Input ───────────────────────────────────────────────────────────────────
//
// Native <input> wrapped with a consistent visual frame (border, focus ring,
// optional prefix slot). All visuals are owned here — consumers express intent
// via `size`, `width`, `prefix` only.
//
// Behavioural HTML attributes (disabled, name, placeholder, min/max/step,
// inputMode, aria-*, onPaste/onClick, …) are forwarded; visual escape hatches
// (className, style) are stripped at the type level.

type Size = 'xs' | 'sm' | 'md'

/**
 * Horizontal extent. Semantic only — not a CSS mirror.
 *   auto    intrinsic
 *   narrow  for number spinners (~80px)
 *   search  responsive search bar (192–224px)
 *   wide    for currency/amount fields (~256px)
 *   full    fill parent
 */
type Width = 'auto' | 'narrow' | 'search' | 'wide' | 'full'

type NativeProps = Omit<
  InputHTMLAttributes<HTMLInputElement>,
  'className' | 'style' | 'onChange' | 'value' | 'size' | 'type' | 'prefix' | 'width'
>

interface InputProps extends NativeProps {
  type?: 'text' | 'number' | 'date' | 'search' | 'email' | 'password' | 'tel'
  size?: Size
  width?: Width
  value: string | number
  onChange: (value: string) => void
  /** Element placed at the start of the input (e.g. currency symbol). */
  prefix?: ReactNode
}

const sizeFrame: Record<Size, string> = {
  xs: 'px-2 py-1 text-xs',
  sm: 'px-3 py-1.5 text-sm',
  md: 'px-3 py-2 text-sm',
}

const prefixPad: Record<Size, string> = {
  xs: 'pl-6',
  sm: 'pl-7',
  md: 'pl-8',
}

const prefixPos: Record<Size, string> = {
  xs: 'left-2 text-xs',
  sm: 'left-2.5 text-sm',
  md: 'left-3 text-sm',
}

const widthClass: Record<Width, string> = {
  auto: '',
  narrow: 'w-20',
  search: 'w-44 sm:w-56',
  wide: 'w-64',
  full: 'w-full',
}

export function Input({
  type = 'text',
  size = 'md',
  width = 'auto',
  value,
  onChange,
  prefix,
  ...rest
}: InputProps) {
  const padding = prefix ? `${sizeFrame[size]} ${prefixPad[size]}` : sizeFrame[size]
  const frame =
    'bg-white border border-slate-200 rounded-lg text-slate-800 placeholder-slate-400 ' +
    'hover:border-slate-300 focus:outline-none focus:ring-2 focus:ring-purple-500/20 focus:border-purple-300 ' +
    'disabled:opacity-50 disabled:cursor-not-allowed transition-colors'

  const w = widthClass[width]

  const input = (
    <input
      {...rest}
      type={type}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className={`${padding} ${frame} ${w}`}
    />
  )

  if (!prefix) return input

  return (
    <div className={`relative inline-block ${w}`}>
      <span className={`absolute top-1/2 -translate-y-1/2 text-slate-500 pointer-events-none ${prefixPos[size]}`}>
        {prefix}
      </span>
      {input}
    </div>
  )
}
