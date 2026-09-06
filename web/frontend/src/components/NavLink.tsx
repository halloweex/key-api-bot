import { memo, useCallback, type ReactNode, type MouseEvent } from 'react'
import { navigate, useRouter } from '../hooks/useRouter'
import { useNavStore } from '../store/navStore'
import type { IconComponent } from './icons'

interface NavLinkProps {
  href: string
  /** Leading icon. Sized by the link itself. */
  icon?: IconComponent
  disabled?: boolean
  children: ReactNode
}

export const NavLink = memo(function NavLink({ href, icon: Icon, disabled, children }: NavLinkProps) {
  const path = useRouter()
  const isActive = path === href
  const setOpen = useNavStore((s) => s.setOpen)

  const handleClick = useCallback((e: MouseEvent) => {
    e.preventDefault()
    navigate(href)
    // Close sidebar on mobile after navigation
    setOpen(false)
  }, [href, setOpen])

  if (disabled) {
    return (
      <div className="flex items-center gap-3 px-3 py-2 rounded-lg text-slate-400 cursor-not-allowed">
        {Icon && <Icon className="w-5 h-5 flex-shrink-0" aria-hidden />}
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
      onClick={handleClick}
      className={`flex items-center gap-3 px-3 py-2 rounded-lg transition-colors
        ${isActive
          ? 'bg-purple-100 text-purple-700 font-medium'
          : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'
        }`}
    >
      {Icon && <Icon className="w-5 h-5 flex-shrink-0" aria-hidden />}
      <span>{children}</span>
    </a>
  )
})
