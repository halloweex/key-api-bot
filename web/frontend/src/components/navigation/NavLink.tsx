import { memo, type ReactNode } from 'react'

interface NavLinkProps {
  href: string
  icon?: ReactNode
  disabled?: boolean
  children: ReactNode
}

export const NavLink = memo(function NavLink({ href, icon, disabled, children }: NavLinkProps) {
  const isActive = window.location.pathname === href

  if (disabled) {
    return (
      <div className="flex items-center gap-3 px-3 py-2 rounded-lg text-slate-400 cursor-not-allowed">
        {icon && <span className="w-5 h-5 flex-shrink-0">{icon}</span>}
        <span className="flex-1">{children}</span>
        <span className="text-xs bg-slate-200 text-slate-500 px-1.5 py-0.5 rounded">
          Soon
        </span>
      </div>
    )
  }

  return (
    <a
      href={href}
      className={`flex items-center gap-3 px-3 py-2 rounded-lg transition-colors
        ${isActive
          ? 'bg-purple-100 text-purple-700 font-medium'
          : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
        }`}
    >
      {icon && <span className="w-5 h-5 flex-shrink-0">{icon}</span>}
      <span>{children}</span>
    </a>
  )
})
