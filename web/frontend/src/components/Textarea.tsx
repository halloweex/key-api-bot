import { useEffect, useRef, type TextareaHTMLAttributes } from 'react'

// ─── Textarea ────────────────────────────────────────────────────────────────
//
// Native <textarea> with consistent frame, focus ring, optional auto-resize.
// Visual is owned entirely here; className/style are stripped at type level.

type Size = 'sm' | 'md'

type NativeProps = Omit<
  TextareaHTMLAttributes<HTMLTextAreaElement>,
  'className' | 'style' | 'onChange' | 'value' | 'size'
>

interface TextareaProps extends NativeProps {
  size?: Size
  value: string
  onChange: (value: string) => void
  /** Auto-grow textarea height to fit content up to `maxHeight` (px). */
  autoResize?: boolean
  /** Used together with `autoResize`. */
  maxHeight?: number
  /** Stretch to fill the parent's width. */
  fullWidth?: boolean
}

const sizeFrame: Record<Size, string> = {
  sm: 'px-3 py-2 text-sm',
  md: 'px-4 py-3 text-sm',
}

export function Textarea({
  size = 'md',
  value,
  onChange,
  autoResize = false,
  maxHeight = 120,
  fullWidth = false,
  ...rest
}: TextareaProps) {
  const ref = useRef<HTMLTextAreaElement>(null)

  useEffect(() => {
    if (!autoResize) return
    const el = ref.current
    if (!el) return
    el.style.height = 'auto'
    el.style.height = `${Math.min(el.scrollHeight, maxHeight)}px`
  }, [value, autoResize, maxHeight])

  const frame =
    'bg-slate-50 border border-slate-200 rounded-2xl text-slate-900 placeholder-slate-400 resize-none ' +
    'hover:border-slate-300 focus:outline-none focus:ring-2 focus:ring-purple-500/40 focus:border-purple-300 focus:bg-white ' +
    'disabled:bg-slate-100 disabled:opacity-60 disabled:cursor-not-allowed transition-all duration-200'

  return (
    <textarea
      {...rest}
      ref={ref}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className={`${sizeFrame[size]} ${frame} ${fullWidth ? 'w-full' : ''}`}
    />
  )
}
