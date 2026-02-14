/**
 * User profile dropdown with avatar, name, role badge, and logout.
 */
import { useState, useRef, useEffect } from 'react'
import { useAuth, useUserDisplayName } from '../../hooks/useAuth'
import type { UserRole } from '../../types/api'

// Role badge colors
const roleBadgeStyles: Record<UserRole, string> = {
  admin: 'bg-purple-100 text-purple-700',
  editor: 'bg-blue-100 text-blue-700',
  viewer: 'bg-slate-100 text-slate-600',
}

const roleLabels: Record<UserRole, string> = {
  admin: 'Admin',
  editor: 'Editor',
  viewer: 'Viewer',
}

export function UserProfileDropdown() {
  const { user, isAuthenticated, isLoading } = useAuth()
  const displayName = useUserDisplayName()
  const [isOpen, setIsOpen] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)

  // Close dropdown when clicking outside
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false)
      }
    }

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside)
      return () => document.removeEventListener('mousedown', handleClickOutside)
    }
  }, [isOpen])

  // Close on escape key
  useEffect(() => {
    function handleEscape(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        setIsOpen(false)
      }
    }

    if (isOpen) {
      document.addEventListener('keydown', handleEscape)
      return () => document.removeEventListener('keydown', handleEscape)
    }
  }, [isOpen])

  // Loading state
  if (isLoading) {
    return (
      <div className="w-9 h-9 rounded-full bg-slate-200 animate-pulse" />
    )
  }

  // Not authenticated - show login link
  if (!isAuthenticated || !user) {
    return (
      <a
        href="/login"
        className="flex items-center gap-2 px-3 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors"
      >
        <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M11 16l-4-4m0 0l4-4m-4 4h14m-5 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h7a3 3 0 013 3v1" />
        </svg>
        <span className="hidden sm:inline">Login</span>
      </a>
    )
  }

  const initials = user.first_name
    ? user.first_name.charAt(0).toUpperCase()
    : user.username
      ? user.username.charAt(0).toUpperCase()
      : '?'

  return (
    <div ref={dropdownRef} className="relative">
      {/* Avatar button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-2 p-1 rounded-full hover:bg-slate-100 transition-colors focus:outline-none focus:ring-2 focus:ring-purple-500/40 focus:ring-offset-2"
        aria-expanded={isOpen}
        aria-haspopup="true"
      >
        {user.photo_url ? (
          <img
            src={user.photo_url}
            alt={displayName}
            className="w-9 h-9 rounded-full object-cover border-2 border-white shadow-sm"
          />
        ) : (
          <div className="w-9 h-9 rounded-full bg-gradient-to-br from-purple-500 to-blue-600 flex items-center justify-center text-white font-semibold text-sm shadow-sm">
            {initials}
          </div>
        )}
        {/* Chevron indicator on desktop */}
        <svg
          className={`w-4 h-4 text-slate-400 transition-transform hidden sm:block ${isOpen ? 'rotate-180' : ''}`}
          fill="none"
          viewBox="0 0 24 24"
          stroke="currentColor"
        >
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
        </svg>
      </button>

      {/* Dropdown menu */}
      {isOpen && (
        <div className="absolute right-0 mt-2 w-64 bg-white rounded-xl shadow-lg border border-slate-200 py-2 z-50 animate-in fade-in slide-in-from-top-2 duration-200">
          {/* User info header */}
          <div className="px-4 py-3 border-b border-slate-100">
            <div className="flex items-center gap-3">
              {user.photo_url ? (
                <img
                  src={user.photo_url}
                  alt={displayName}
                  className="w-10 h-10 rounded-full object-cover"
                />
              ) : (
                <div className="w-10 h-10 rounded-full bg-gradient-to-br from-purple-500 to-blue-600 flex items-center justify-center text-white font-semibold">
                  {initials}
                </div>
              )}
              <div className="min-w-0 flex-1">
                <p className="text-sm font-semibold text-slate-900 truncate">
                  {displayName}
                </p>
                {user.username && (
                  <p className="text-xs text-slate-500 truncate">
                    @{user.username}
                  </p>
                )}
              </div>
            </div>
            {/* Role badge */}
            <div className="mt-2">
              <span className={`inline-flex items-center px-2 py-0.5 rounded-md text-xs font-medium ${roleBadgeStyles[user.role]}`}>
                {roleLabels[user.role]}
              </span>
            </div>
          </div>

          {/* Menu items */}
          <div className="py-1">
            {/* Admin users page link (admin only) */}
            {user.role === 'admin' && (
              <a
                href="/v2/admin/users"
                className="flex items-center gap-3 px-4 py-2 text-sm text-slate-700 hover:bg-slate-50 transition-colors"
                onClick={() => setIsOpen(false)}
              >
                <svg className="w-4 h-4 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" />
                </svg>
                Manage Users
              </a>
            )}

            {/* Logout */}
            <a
              href="/logout"
              className="flex items-center gap-3 px-4 py-2 text-sm text-red-600 hover:bg-red-50 transition-colors"
              onClick={() => setIsOpen(false)}
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M17 16l4-4m0 0l-4-4m4 4H7m6 4v1a3 3 0 01-3 3H6a3 3 0 01-3-3V7a3 3 0 013-3h4a3 3 0 013 3v1" />
              </svg>
              Logout
            </a>
          </div>
        </div>
      )}
    </div>
  )
}
