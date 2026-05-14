import { type SelectHTMLAttributes, type CSSProperties } from 'react'

// ─── Select ──────────────────────────────────────────────────────────────────
//
// Native <select> with consistent visuals. Variants:
//   framed  (default) — white border + shadow, mobile-tap target, chevron
//   compact          — smaller white box, no min-height, smaller chevron
//   pill             — slate-100 background, no border, no chevron
//
// className/style are stripped at the type level so consumers can't override
// the look.

type Variant = 'framed' | 'compact' | 'pill'

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
  variant?: Variant
}

const CHEVRON_STYLE: CSSProperties = {
  backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 24 24' stroke='%2394a3b8'%3E%3Cpath stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M19 9l-7 7-7-7'%3E%3C/path%3E%3C/svg%3E")`,
  backgroundPosition: 'right 0.5rem center',
  backgroundSize: '1.25rem',
  paddingRight: '2.25rem',
}

const COMPACT_CHEVRON_STYLE: CSSProperties = {
  backgroundImage: CHEVRON_STYLE.backgroundImage,
  backgroundPosition: 'right 0.25rem center',
  backgroundSize: '1rem',
  paddingRight: '1.5rem',
}

const variantClass: Record<Variant, string> = {
  framed:
    'px-3 py-2.5 sm:py-2 bg-white border border-slate-200 rounded-lg text-sm text-slate-700 font-medium ' +
    'shadow-sm hover:border-slate-300 hover:shadow ' +
    'focus:outline-none focus:ring-2 focus:ring-purple-500/40 focus:border-purple-400 focus:ring-offset-1 ' +
    'transition-all duration-200 cursor-pointer appearance-none bg-no-repeat bg-right ' +
    'min-w-0 w-full sm:w-auto sm:min-w-[120px] min-h-[44px] sm:min-h-[40px]',
  compact:
    'px-2 py-1 text-sm bg-white border border-slate-200 rounded-md text-slate-700 ' +
    'hover:border-slate-300 ' +
    'focus:outline-none focus:ring-2 focus:ring-purple-500/30 focus:border-purple-400 ' +
    'cursor-pointer appearance-none bg-no-repeat bg-right transition-colors',
  pill:
    'text-[10px] sm:text-xs bg-slate-100 border-0 rounded-lg px-1.5 sm:px-2.5 py-1.5 text-slate-600 font-medium ' +
    'hover:bg-slate-200 ' +
    'focus:outline-none focus:ring-2 focus:ring-purple-500/40 ' +
    'cursor-pointer appearance-none transition-colors',
}

const variantStyle: Record<Variant, CSSProperties> = {
  framed: CHEVRON_STYLE,
  compact: COMPACT_CHEVRON_STYLE,
  pill: {},
}

export function Select({
  options,
  value,
  onChange,
  placeholder,
  allowEmpty = true,
  emptyLabel = 'All',
  variant = 'framed',
  ...rest
}: SelectProps) {
  return (
    <select
      {...rest}
      value={value ?? ''}
      onChange={(e) => onChange(e.target.value || null)}
      className={variantClass[variant]}
      style={variantStyle[variant]}
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
