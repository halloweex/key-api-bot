import { memo } from 'react'
import { Checkbox } from '../ui/Checkbox'

interface PermissionCheckboxProps {
  checked: boolean
  onChange: () => void
  disabled: boolean
  label: string
}

export const PermissionCheckbox = memo(function PermissionCheckbox({
  checked,
  onChange,
  disabled,
  label,
}: PermissionCheckboxProps) {
  return (
    <label className={`flex items-center gap-1.5 cursor-pointer ${disabled ? 'opacity-50 cursor-not-allowed' : ''}`}>
      <Checkbox checked={checked} onChange={onChange} disabled={disabled} size="md" />
      <span className="text-xs text-slate-600">{label}</span>
    </label>
  )
})
