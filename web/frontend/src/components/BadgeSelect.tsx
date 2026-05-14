import { type SelectHTMLAttributes } from 'react'

// ─── BadgeSelect ─────────────────────────────────────────────────────────────
//
// A select that visually reads as a Badge — coloured pill with a border,
// suitable for inline use inside table cells (e.g. user role / status
// switching). Tone is fully owned here, mapped from a semantic prop.

type Tone = 'purple' | 'blue' | 'slate' | 'green' | 'yellow' | 'red'

interface BadgeSelectOption {
  value: string
  label: string
}

type NativeProps = Omit<
  SelectHTMLAttributes<HTMLSelectElement>,
  'className' | 'style' | 'onChange' | 'value' | 'children'
>

interface BadgeSelectProps extends NativeProps {
  options: BadgeSelectOption[]
  value: string
  onChange: (value: string) => void
  tone: Tone
}

const toneClass: Record<Tone, string> = {
  purple: 'bg-purple-100 text-purple-700 border-purple-200',
  blue: 'bg-blue-100 text-blue-700 border-blue-200',
  slate: 'bg-slate-100 text-slate-600 border-slate-200',
  green: 'bg-green-100 text-green-700 border-green-200',
  yellow: 'bg-yellow-100 text-yellow-700 border-yellow-200',
  red: 'bg-red-100 text-red-700 border-red-200',
}

export function BadgeSelect({ options, value, onChange, tone, ...rest }: BadgeSelectProps) {
  return (
    <select
      {...rest}
      value={value}
      onChange={(e) => onChange(e.target.value)}
      className={`px-2 py-1 text-xs font-medium rounded-md border cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed ${toneClass[tone]}`}
    >
      {options.map((opt) => (
        <option key={opt.value} value={opt.value}>
          {opt.label}
        </option>
      ))}
    </select>
  )
}
