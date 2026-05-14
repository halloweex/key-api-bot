import { memo } from 'react'
import { Select } from './Select'

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
  const stringOptions = options.map((opt) => ({
    value: String(opt.value),
    label: `${opt.label}${suffix ? ` ${suffix}` : ''}`,
  }))
  return (
    <div className="flex items-center gap-1.5 text-sm">
      <span className="text-slate-500 font-medium">{label}:</span>
      <Select
        variant="compact"
        options={stringOptions}
        value={String(value)}
        onChange={(v) => v && onChange(Number(v))}
        allowEmpty={false}
      />
    </div>
  )
})
