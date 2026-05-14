import { memo, type ReactNode } from 'react'

// ─── ExportCsvButton ─────────────────────────────────────────────────────────
//
// Subtle purple chip button used for CSV/data export actions.
// Visual is owned entirely here — consumers pass only behaviour (onClick) and
// label/children. No className/style escape.

interface ExportCsvButtonProps {
  onClick: () => void
  children: ReactNode
  disabled?: boolean
}

export const ExportCsvButton = memo(function ExportCsvButton({
  onClick,
  children,
  disabled = false,
}: ExportCsvButtonProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      className="text-xs font-medium text-purple-600 hover:text-purple-700 bg-purple-50 hover:bg-purple-100 px-3 py-1.5 rounded-lg transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
    >
      {children}
    </button>
  )
})
