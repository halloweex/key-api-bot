import { memo } from 'react'

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
      <input
        type="checkbox"
        checked={checked}
        onChange={onChange}
        disabled={disabled}
        className="w-4 h-4 rounded border-slate-300 text-purple-600 focus:ring-purple-500 focus:ring-offset-0 disabled:cursor-not-allowed"
      />
      <span className="text-xs text-slate-600">{label}</span>
    </label>
  )
})
