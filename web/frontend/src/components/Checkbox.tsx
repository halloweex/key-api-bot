import { type InputHTMLAttributes } from 'react'

// ─── Checkbox ────────────────────────────────────────────────────────────────
//
// Native checkbox with a consistent visual (size, focus ring). Visual is owned
// entirely here — consumers express intent via `size` and behavioural attrs.
// className/style are stripped at the type level.

type Size = 'sm' | 'md'

type NativeProps = Omit<
  InputHTMLAttributes<HTMLInputElement>,
  'className' | 'style' | 'type' | 'onChange' | 'checked' | 'size'
>

interface CheckboxProps extends NativeProps {
  checked: boolean
  onChange: (checked: boolean) => void
  size?: Size
}

const sizeClass: Record<Size, string> = {
  sm: 'w-3.5 h-3.5',
  md: 'w-4 h-4',
}

export function Checkbox({ checked, onChange, size = 'md', ...rest }: CheckboxProps) {
  return (
    <input
      {...rest}
      type="checkbox"
      checked={checked}
      onChange={(e) => onChange(e.target.checked)}
      className={`${sizeClass[size]} rounded border-slate-300 text-purple-600 focus:ring-purple-500 focus:ring-offset-0 disabled:cursor-not-allowed disabled:opacity-50`}
    />
  )
}
