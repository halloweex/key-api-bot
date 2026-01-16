import { type SelectHTMLAttributes, forwardRef } from 'react'

interface SelectOption {
  value: string
  label: string
}

interface SelectProps extends Omit<SelectHTMLAttributes<HTMLSelectElement>, 'onChange' | 'value'> {
  options: SelectOption[]
  value: string | null
  onChange: (value: string | null) => void
  placeholder?: string
  allowEmpty?: boolean
  emptyLabel?: string
}

export const Select = forwardRef<HTMLSelectElement, SelectProps>(
  ({ options, value, onChange, placeholder, allowEmpty = true, emptyLabel = 'All', className = '', ...props }, ref) => {
    return (
      <select
        ref={ref}
        value={value ?? ''}
        onChange={(e) => onChange(e.target.value || null)}
        className={`
          px-3 py-2 bg-white border border-slate-300 rounded-lg
          text-sm text-slate-900
          focus:outline-none focus:ring-2 focus:ring-blue-500/50 focus:border-blue-500
          transition-colors cursor-pointer
          ${className}
        `}
        {...props}
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
)

Select.displayName = 'Select'
