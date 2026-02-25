import { memo, useEffect, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useNavStore } from '../../store/navStore'
import { useAuth, useUserDisplayName } from '../../hooks/useAuth'
import { useWebSocket } from '../../hooks/useWebSocket'
import { api } from '../../api/client'
import { NavLink } from './NavLink'
import { UserProfileDropdown } from '../ui/UserProfileDropdown'
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

// ─── Icons ───────────────────────────────────────────────────────────────────

const PanelLeftIcon = () => (
  <svg className="h-5 w-5" viewBox="0 0 24 24">
    <rect x="2" y="2" width="20" height="20" rx="5" fill="none" stroke="currentColor" strokeWidth={1.5} />
    <rect x="2" y="2" width="8" height="20" rx="5" fill="currentColor" fillOpacity="0.2" />
    <line x1="10" y1="4" x2="10" y2="20" stroke="currentColor" strokeWidth={2} strokeLinecap="round" />
  </svg>
)

const ChartBarIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
  </svg>
)

const MegaphoneIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M11 5.882V19.24a1.76 1.76 0 01-3.417.592l-2.147-6.15M18 13a3 3 0 100-6M5.436 13.683A4.001 4.001 0 017 6h1.832c4.1 0 7.625-1.234 9.168-3v14c-1.543-1.766-5.067-3-9.168-3H7a3.988 3.988 0 01-1.564-.317z" />
  </svg>
)

const CurrencyDollarIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
  </svg>
)

const CubeIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4" />
  </svg>
)

const ClipboardDocIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M9 12h3.75M9 15h3.75M9 18h3.75m3 .75H18a2.25 2.25 0 002.25-2.25V6.108c0-1.135-.845-2.098-1.976-2.192a48.424 48.424 0 00-1.123-.08m-5.801 0c-.065.21-.1.433-.1.664 0 .414.336.75.75.75h4.5a.75.75 0 00.75-.75 2.25 2.25 0 00-.1-.664m-5.8 0A2.251 2.251 0 0113.5 2.25H15c1.012 0 1.867.668 2.15 1.586m-5.8 0c-.376.023-.75.05-1.124.08C9.095 4.01 8.25 4.973 8.25 6.108V8.25m0 0H4.875c-.621 0-1.125.504-1.125 1.125v11.25c0 .621.504 1.125 1.125 1.125h9.75c.621 0 1.125-.504 1.125-1.125V9.375c0-.621-.504-1.125-1.125-1.125H8.25z" />
  </svg>
)

const SignalIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M3 4h13M3 8h9m-9 4h6m4 0l4-4m0 0l4 4m-4-4v12" />
  </svg>
)

const LightBulbIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
  </svg>
)

const UsersIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" />
  </svg>
)

const ShieldCheckIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
  </svg>
)

// ─── Component ───────────────────────────────────────────────────────────────

export const SidebarRail = memo(function SidebarRail() {
  const { isOpen, toggleOpen, setOpen } = useNavStore()
  const { user, isAuthenticated } = useAuth()
  const displayName = useUserDisplayName()
  const isAdmin = user?.role === 'admin'
  const [imageError, setImageError] = useState(false)

  // Get 2-letter initials from name, username, or fallback
  const getInitials = () => {
    if (!user) return '??'
    if (user.first_name && user.last_name) {
      return (user.first_name.charAt(0) + user.last_name.charAt(0)).toUpperCase()
    }
    if (user.first_name && user.first_name.length >= 2) {
      return user.first_name.substring(0, 2).toUpperCase()
    }
    if (user.username && user.username.length >= 2) {
      return user.username.substring(0, 2).toUpperCase()
    }
    return user.first_name?.charAt(0).toUpperCase() || user.username?.charAt(0).toUpperCase() || '??'
  }

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
        <PanelLeftIcon />
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
            title="Collapse menu (Esc)"
            aria-label="Collapse navigation"
          >
            <PanelLeftIcon />
          </button>
        </div>
      </div>

      {/* Expanded content */}
      <div
        className={`mt-2 px-3 flex flex-col h-[calc(100%-4.5rem)]
          ${isOpen ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
      >
        {/* Main navigation */}
        <nav className="space-y-1" aria-label="Dashboard pages">
          <NavLink href="/v2" icon={<ChartBarIcon />}>
            Sales Dashboard
          </NavLink>
          <NavLink href="/v2/products" icon={<LightBulbIcon />}>
            Product Intelligence
          </NavLink>
          <NavLink href="/v2/traffic" icon={<SignalIcon />}>
            Traffic Analytics
          </NavLink>
          <NavLink href="/v2/marketing" icon={<MegaphoneIcon />} disabled>
            Marketing
          </NavLink>
          <NavLink href="/v2/financial" icon={<CurrencyDollarIcon />} disabled>
            Financial
          </NavLink>
          <NavLink href="/v2/inventory" icon={<CubeIcon />}>
            Inventory
          </NavLink>
          <NavLink href="/v2/reports" icon={<ClipboardDocIcon />}>
            Reports
          </NavLink>
        </nav>

        {/* Admin section */}
        {isAdmin && (
          <div className="mt-6 pt-4 border-t border-slate-200">
            <p className="text-xs text-slate-400 uppercase tracking-wide mb-2 px-3 font-medium">
              Admin
            </p>
            <nav className="space-y-1" aria-label="Admin pages">
              <NavLink href="/v2/admin/users" icon={<UsersIcon />}>
                Manage Users
              </NavLink>
              <NavLink href="/v2/admin/permissions" icon={<ShieldCheckIcon />}>
                Permissions
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
          <div className="relative w-8 h-8">
            {/* Initials background - always visible */}
            <div className="absolute inset-0 rounded-full bg-gradient-to-br from-purple-500 to-blue-600 flex items-center justify-center text-white font-bold text-xs shadow-sm">
              {getInitials()}
            </div>
            {/* Photo overlay - only if available and not errored */}
            {user.photo_url && !imageError && (
              <img
                src={user.photo_url}
                alt=""
                className="absolute inset-0 w-8 h-8 rounded-full object-cover border-2 border-white shadow-sm"
                onError={() => setImageError(true)}
              />
            )}
          </div>
        ) : (
          <div className="w-8 h-8 rounded-full bg-gradient-to-br from-purple-500 to-blue-600 flex items-center justify-center text-white text-xs font-bold shadow-sm">
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
            </svg>
          </div>
        )}
      </div>
    </aside>
    </>
  )
})
