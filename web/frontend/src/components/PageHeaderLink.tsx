import { memo, type ReactNode } from 'react'

// ─── PageHeaderLink ──────────────────────────────────────────────────────────
//
// Utility navigation link used in page headers (e.g. "← Dashboard",
// "Permissions →"). Visual is owned here — consumers pass only href, icon
// and label. No className/style escape.

interface PageHeaderLinkProps {
  href: string
  icon: ReactNode
  children: ReactNode
}

export const PageHeaderLink = memo(function PageHeaderLink({
  href,
  icon,
  children,
}: PageHeaderLinkProps) {
  return (
    <a
      href={href}
      className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors"
    >
      {icon}
      {children}
    </a>
  )
})
