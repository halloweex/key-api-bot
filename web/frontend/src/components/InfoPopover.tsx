import { useState, useRef, useEffect, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { CircleHelp } from 'lucide-react'

/**
 * A "?" info button that toggles a dark popover panel on click.
 * Matches the existing InfoButton + InfoTooltipContent pattern
 * used in RevenueTrendChart and CustomerInsightsChart.
 */
export function InfoPopover({ title, children }: {
  title?: string
  children: ReactNode
}) {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!open) return
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false)
      }
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  return (
    <div className="relative" ref={ref}>
      <button
        onClick={() => setOpen(!open)}
        className="text-slate-400 hover:text-slate-600 transition-colors"
        aria-label={t('common.moreInfo')}
      >
        <CircleHelp className="w-4 h-4" />
      </button>
      {open && (
        <div className="absolute top-8 left-0 z-50 bg-slate-800 border border-slate-700 rounded-lg shadow-xl p-4 min-w-[220px] max-w-[300px]">
          <button
            onClick={() => setOpen(false)}
            className="absolute top-2 right-2 text-slate-400 hover:text-slate-200 text-lg leading-none"
            aria-label="Close"
          >
            ×
          </button>
          {title && <h4 className="text-sm font-semibold text-slate-200 mb-2">{title}</h4>}
          {children}
        </div>
      )}
    </div>
  )
}
