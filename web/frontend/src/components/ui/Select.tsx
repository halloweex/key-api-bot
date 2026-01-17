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
          px-3 py-2 bg-white border border-slate-200 rounded-lg
          text-sm text-slate-700 font-medium
          shadow-sm hover:border-slate-300
          focus:outline-none focus:ring-2 focus:ring-blue-500/40 focus:border-blue-400 focus:ring-offset-1
          transition-all duration-200 cursor-pointer
          appearance-none bg-no-repeat bg-right
          ${className}
        `}
        style={{
          backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 24 24' stroke='%2394a3b8'%3E%3Cpath stroke-linecap='round' stroke-linejoin='round' stroke-width='2' d='M19 9l-7 7-7-7'%3E%3C/path%3E%3C/svg%3E")`,
          backgroundPosition: 'right 0.5rem center',
          backgroundSize: '1.25rem',
          paddingRight: '2.25rem',
        }}
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
