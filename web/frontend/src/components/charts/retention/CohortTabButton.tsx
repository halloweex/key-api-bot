import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import type { Tab } from './tabsConfig'

interface CohortTabButtonProps {
  tab: Tab
  isActive: boolean
  onClick: () => void
}

export const CohortTabButton = memo(function CohortTabButton({
  tab,
  isActive,
  onClick,
}: CohortTabButtonProps) {
  const { t } = useTranslation()
  return (
    <button
      onClick={onClick}
      className={`
        px-3 py-2 text-sm font-medium rounded-lg transition-colors whitespace-nowrap
        ${isActive
          ? 'bg-blue-100 text-blue-700 border border-blue-200'
          : 'text-slate-600 hover:bg-slate-100 border border-transparent'
        }
      `}
    >
      <span className="hidden sm:inline">{t(tab.label)}</span>
      <span className="sm:hidden">{t(tab.shortLabel)}</span>
    </button>
  )
})
