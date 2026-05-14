import { memo } from 'react'

// ─── RoleLegendChip ──────────────────────────────────────────────────────────
//
// Tone-coloured chip with a bold label and optional secondary description,
// used in the role legend above the permissions matrix. Larger / framed
// shape than Badge (rounded-lg + border + heavier padding) — distinct
// purpose from Badge / BadgeSelect.

type Tone = 'purple' | 'blue' | 'slate' | 'green' | 'yellow' | 'red'

interface RoleLegendChipProps {
  tone: Tone
  label: string
  description?: string
}

const toneClass: Record<Tone, string> = {
  purple: 'bg-purple-100 text-purple-700 border-purple-200',
  blue: 'bg-blue-100 text-blue-700 border-blue-200',
  slate: 'bg-slate-100 text-slate-600 border-slate-200',
  green: 'bg-green-100 text-green-700 border-green-200',
  yellow: 'bg-yellow-100 text-yellow-700 border-yellow-200',
  red: 'bg-red-100 text-red-700 border-red-200',
}

export const RoleLegendChip = memo(function RoleLegendChip({
  tone,
  label,
  description,
}: RoleLegendChipProps) {
  return (
    <div className={`px-3 py-1.5 rounded-lg border ${toneClass[tone]}`}>
      <span className="font-medium">{label}</span>
      {description && (
        <span className="ml-2 text-xs opacity-75">{description}</span>
      )}
    </div>
  )
})
