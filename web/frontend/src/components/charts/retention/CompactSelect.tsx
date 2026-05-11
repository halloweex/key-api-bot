import { memo } from 'react'

interface CompactSelectProps {
  label: string
  value: number
  options: { value: number; label: string }[]
  onChange: (v: number) => void
  suffix?: string
}

export const CompactSelect = memo(function CompactSelect({
  label,
  value,
  options,
  onChange,
  suffix,
}: CompactSelectProps) {
  return (
    <div className="flex items-center gap-1.5 text-sm">
      <span className="text-slate-500 font-medium">{label}:</span>
      <select
        value={value}
        onChange={(e) => onChange(Number(e.target.value))}
        className="bg-white border border-slate-200 rounded-md px-2 py-1 text-sm text-slate-700 focus:ring-1 focus:ring-blue-400 focus:border-blue-400 outline-none"
      >
        {options.map((opt) => (
          <option key={opt.value} value={opt.value}>
            {opt.label}{suffix ? ` ${suffix}` : ''}
          </option>
        ))}
      </select>
    </div>
  )
})
