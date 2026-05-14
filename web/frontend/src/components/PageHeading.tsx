import { memo, type ReactNode } from 'react'

// ─── PageHeading ─────────────────────────────────────────────────────────────
//
// Title + optional subtitle pair used at the top of pages. Visual owned here:
// page titles are always one size, subtitles one weight. Pages express intent
// via two strings; the optional `actions` slot is reserved for nav/utility
// links composed by the page itself.

interface PageHeadingProps {
  title: string
  subtitle?: string
  /** Optional right-aligned slot for header utility actions (PageHeaderLink, etc.) */
  actions?: ReactNode
}

export const PageHeading = memo(function PageHeading({
  title,
  subtitle,
  actions,
}: PageHeadingProps) {
  return (
    <div className="flex items-center justify-between mb-6">
      <div>
        <h1 className="text-2xl font-bold text-slate-900">{title}</h1>
        {subtitle && <p className="text-sm text-slate-500 mt-1">{subtitle}</p>}
      </div>
      {actions && <div className="flex gap-3">{actions}</div>}
    </div>
  )
})
