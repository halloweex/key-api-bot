import { memo, useEffect, type ReactNode } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useTranslation } from 'react-i18next'
import {
  BarChart3, Lightbulb, Activity, Box, ClipboardList, Rocket,
  CircleDollarSign, Users, ShieldCheck, PanelLeft, User,
} from 'lucide-react'
import { useNavStore } from '../../store/navStore'
import { useAuth, useUserDisplayName } from '../../hooks/useAuth'
import { useWebSocket } from '../../hooks/useWebSocket'
import { api } from '../../api/client'
import { navigate, useRouter } from '../../hooks/useRouter'
import { NavLink } from './NavLink'
import { UserProfileDropdown } from '../ui/UserProfileDropdown'
import { UserAvatar } from '../ui/UserAvatar'
import { LiveIndicator } from '../ui/LiveIndicator'
import type { HealthResponse } from '../../types/api'

// ─── Logo ────────────────────────────────────────────────────────────────────

const Logo = ({ size = 32 }: { size?: number }) => (
  <svg
    viewBox="0 0 24 24"
    width={size}
    height={size}
    style={{ width: size, height: size, minWidth: size, minHeight: size }}
  >
    <defs>
      <linearGradient id="logoGrad" x1="0%" y1="0%" x2="100%" y2="100%">
        <stop offset="0%" stopColor="#8B5CF6"/>
        <stop offset="100%" stopColor="#2563EB"/>
      </linearGradient>
    </defs>
    <rect width="24" height="24" rx="5" fill="url(#logoGrad)"/>
    <path fill="#fff" d="M9 17H7v-7h2v7zm4 0h-2V7h2v10zm4 0h-2v-4h2v4z"/>
  </svg>
)

// ─── Collapsed Nav Icon ──────────────────────────────────────────────────────

function CollapsedNavIcon({ href, icon, label }: { href: string; icon: ReactNode; label: string }) {
  const path = useRouter()
  const isActive = path === href

  const handleClick = (e: React.MouseEvent) => {
    e.preventDefault()
    e.stopPropagation()
    navigate(href)
  }

  return (
    <a
      href={href}
      onClick={handleClick}
      title={label}
      className={`w-9 h-9 flex items-center justify-center rounded-lg transition-colors ${
        isActive
          ? 'bg-purple-100 text-purple-700'
          : 'text-slate-500 hover:bg-slate-200 hover:text-slate-700'
      }`}
    >
      {icon}
    </a>
  )
}

// ─── Component ───────────────────────────────────────────────────────────────

export const SidebarRail = memo(function SidebarRail() {
  const { t } = useTranslation()
  const { isOpen, toggleOpen, setOpen } = useNavStore()
  const { user, isAuthenticated } = useAuth()
  const displayName = useUserDisplayName()
  const isAdmin = user?.role === 'admin'

  // Health check for version
  const { data: health } = useQuery<HealthResponse>({
    queryKey: ['health'],
    queryFn: () => api.getHealth(),
    staleTime: 5 * 60 * 1000,
    refetchOnWindowFocus: false,
  })

  // WebSocket for real-time updates
  const { connectionState, lastMessageTime } = useWebSocket({
    enabled: true,
    room: 'dashboard',
  })

  // Close on Escape
  useEffect(() => {
    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && isOpen) {
        setOpen(false)
      }
    }
    document.addEventListener('keydown', handleEscape)
    return () => document.removeEventListener('keydown', handleEscape)
  }, [isOpen, setOpen])

  // Keyboard shortcut: Cmd/Ctrl + M
  useEffect(() => {
    const handleShortcut = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'm') {
        e.preventDefault()
        toggleOpen()
      }
    }
    document.addEventListener('keydown', handleShortcut)
    return () => document.removeEventListener('keydown', handleShortcut)
  }, [toggleOpen])

  return (
    <>
      {/* Mobile backdrop */}
      <div
        className={`fixed inset-0 bg-black/30 z-[54] sm:hidden
          transition-opacity duration-200
          ${isOpen ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
        onClick={() => setOpen(false)}
        aria-hidden="true"
      />

      {/* Mobile toggle button - top left */}
      <button
        onClick={() => setOpen(true)}
        className={`fixed top-3 left-3 z-[53] sm:hidden
          w-8 h-8 rounded-lg bg-white border border-slate-200 shadow-sm
          flex items-center justify-center text-slate-500
          hover:bg-slate-50 active:bg-slate-100
          ${isOpen ? 'opacity-0 pointer-events-none' : 'opacity-100'}`}
        aria-label="Open menu"
      >
        <PanelLeft className="h-5 w-5" />
      </button>

      <aside
        className={`fixed top-0 left-0 bottom-0 z-[55]
          bg-slate-50 border-r border-slate-200
          transition-transform duration-200 ease-out
          ${isOpen
            ? 'translate-x-0 w-[280px]'
            : '-translate-x-full sm:translate-x-0 sm:w-12 sm:cursor-pointer sm:hover:bg-slate-100'
          }`}
        role="navigation"
        aria-label="Main navigation"
        onClick={isOpen ? undefined : toggleOpen}
      >
      {/* Header with logo - always visible */}
      <div className="h-14 flex items-center border-b border-slate-200">
        {/* Collapsed: centered logo */}
        <div
          className={`absolute left-0 right-0 flex justify-center
            ${isOpen ? 'opacity-0 pointer-events-none' : 'opacity-100'}`}
        >
          <div className="p-1.5">
            <Logo size={28} />
          </div>
        </div>

        {/* Expanded: logo + name + toggle */}
        <div
          className={`flex items-center justify-between w-full px-3
            ${isOpen ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
        >
          <div className="flex items-center gap-2.5">
            <div className="shadow-md overflow-hidden flex-shrink-0">
              <Logo size={32} />
            </div>
            <div className="min-w-0">
              <h1 className="text-sm font-semibold text-slate-900">
                KoreanStory
              </h1>
              <p className="text-[10px] text-slate-500 truncate">
                {isAuthenticated && displayName ? displayName : 'Analytics'}
              </p>
            </div>
          </div>
          <button
            onClick={toggleOpen}
            className="p-1.5 rounded-lg hover:bg-slate-200 text-slate-500 hover:text-slate-700"
            title={t('nav.collapseMenu')}
            aria-label="Collapse navigation"
          >
            <PanelLeft className="h-5 w-5" />
          </button>
        </div>
      </div>

      {/* Collapsed: icon-only nav */}
      <div
        className={`absolute top-14 left-0 right-0 hidden sm:flex flex-col items-center gap-1 pt-2 px-1
          ${isOpen ? 'opacity-0 pointer-events-none' : 'opacity-100'}`}
      >
        <CollapsedNavIcon href="/v2" icon={<BarChart3 className="w-5 h-5" />} label={t('nav.salesDashboard')} />
        <CollapsedNavIcon href="/v2/products" icon={<Lightbulb className="w-5 h-5" />} label={t('nav.productIntelligence')} />
        <CollapsedNavIcon href="/v2/traffic" icon={<Activity className="w-5 h-5" />} label={t('nav.trafficAnalytics')} />
        <CollapsedNavIcon href="/v2/inventory" icon={<Box className="w-5 h-5" />} label={t('nav.inventory')} />
        <CollapsedNavIcon href="/v2/reports" icon={<ClipboardList className="w-5 h-5" />} label={t('nav.reports')} />
        <CollapsedNavIcon href="/v2/marketing" icon={<Rocket className="w-5 h-5" />} label={t('nav.marketing')} />
      </div>

      {/* Expanded content */}
      <div
        className={`mt-2 px-3 flex flex-col h-[calc(100%-4.5rem)]
          ${isOpen ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
      >
        {/* Main navigation */}
        <nav className="space-y-1" aria-label="Dashboard pages">
          <NavLink href="/v2" icon={<BarChart3 className="w-5 h-5" />}>
            {t('nav.salesDashboard')}
          </NavLink>
          <NavLink href="/v2/products" icon={<Lightbulb className="w-5 h-5" />}>
            {t('nav.productIntelligence')}
          </NavLink>
          <NavLink href="/v2/traffic" icon={<Activity className="w-5 h-5" />}>
            {t('nav.trafficAnalytics')}
          </NavLink>
          <NavLink href="/v2/inventory" icon={<Box className="w-5 h-5" />}>
            {t('nav.inventory')}
          </NavLink>
          <NavLink href="/v2/reports" icon={<ClipboardList className="w-5 h-5" />}>
            {t('nav.reports')}
          </NavLink>
          <NavLink href="/v2/marketing" icon={<Rocket className="w-5 h-5" />}>
            {t('nav.marketing')}
          </NavLink>
          <NavLink href="/v2/financial" icon={<CircleDollarSign className="w-5 h-5" />} disabled>
            {t('nav.financial')}
          </NavLink>
        </nav>

        {/* Admin section */}
        {isAdmin && (
          <div className="mt-6 pt-4 border-t border-slate-200">
            <p className="text-xs text-slate-400 uppercase tracking-wide mb-2 px-3 font-medium">
              {t('nav.admin')}
            </p>
            <nav className="space-y-1" aria-label="Admin pages">
              <NavLink href="/v2/admin/users" icon={<Users className="w-5 h-5" />}>
                {t('nav.manageUsers')}
              </NavLink>
              <NavLink href="/v2/admin/permissions" icon={<ShieldCheck className="w-5 h-5" />}>
                {t('nav.permissions')}
              </NavLink>
            </nav>
          </div>
        )}

        {/* Spacer to push status and user profile to bottom */}
        <div className="flex-1" />

        {/* Status section */}
        <div className="py-3 border-t border-slate-200">
          <div className="flex items-center justify-between px-1">
            <LiveIndicator
              connectionState={connectionState}
              lastMessageTime={lastMessageTime}
            />
            {health?.version && (
              <span className="text-[10px] text-slate-400 font-medium bg-slate-100 px-1.5 py-0.5 rounded-md">
                v{health.version}
              </span>
            )}
          </div>
        </div>

        {/* User profile at bottom */}
        <div className="py-3 border-t border-slate-200">
          <UserProfileDropdown />
        </div>
      </div>

      {/* Collapsed: user icon at bottom */}
      <div
        className={`absolute bottom-4 left-0 right-0 flex justify-center
          ${isOpen ? 'opacity-0 pointer-events-none' : 'opacity-100'}`}
      >
        {isAuthenticated && user ? (
          <UserAvatar
            name={displayName || user.username || 'User'}
            photoUrl={user.photo_url}
            size={32}
          />
        ) : (
          <div className="w-8 h-8 rounded-full bg-gradient-to-br from-purple-500 to-blue-600 flex items-center justify-center text-white text-xs font-bold shadow-sm">
            <User className="w-4 h-4" />
          </div>
        )}
      </div>
    </aside>
    </>
  )
})
