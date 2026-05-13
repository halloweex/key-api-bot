import { type SelectHTMLAttributes } from 'react'

// ─── Select ──────────────────────────────────────────────────────────────────
//
// Native <select> wrapped with a consistent visual frame (border, focus ring,
// custom chevron). The chevron arrow is baked in as an inline-SVG background
// so the look is uniform across browsers without a portal/listbox layer.
//
// Behavioural HTML attributes (disabled, name, required, aria-*, …) are
// forwarded; visual escape hatches (className, style) are stripped at the
// type level so consumers can't override the look.

interface SelectOption {
  value: string
  label: string
}

type NativeProps = Omit<
  SelectHTMLAttributes<HTMLSelectElement>,
  'className' | 'style' | 'onChange' | 'value' | 'children'
>

interface SelectProps extends NativeProps {
  options: SelectOption[]
  value: string | null
  onChange: (value: string | null) => void
  placeholder?: string
  allowEmpty?: boolean
  emptyLabel?: string
}

const CHEVRON_STYLE = {
  backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 24 24' stroke='%2394a3b8'%3E%3Cpath stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M19 9l-7 7-7-7'%3E%3C/path%3E%3C/svg%3E")`,
  backgroundPosition: 'right 0.5rem center',
  backgroundSize: '1.25rem',
  paddingRight: '2.25rem',
} as const

export function Select({
  options,
  value,
  onChange,
  placeholder,
  allowEmpty = true,
  emptyLabel = 'All',
  ...rest
}: SelectProps) {
  return (
    <select
      {...rest}
      value={value ?? ''}
      onChange={(e) => onChange(e.target.value || null)}
      className="px-3 py-2.5 sm:py-2 bg-white border border-slate-200 rounded-lg text-sm text-slate-700 font-medium shadow-sm hover:border-slate-300 hover:shadow focus:outline-none focus:ring-2 focus:ring-purple-500/40 focus:border-purple-400 focus:ring-offset-1 transition-all duration-200 cursor-pointer appearance-none bg-no-repeat bg-right min-w-0 w-full sm:w-auto sm:min-w-[120px] min-h-[44px] sm:min-h-[40px]"
      style={CHEVRON_STYLE}
    >
      {allowEmpty && (
        <option value="">{placeholder ?? emptyLabel}</option>
      )}
      {options.map((option) => (
        <option key={option.value} value={option.value}>
          {option.label}
        </option>
      ))}
    </select>
  )
}
