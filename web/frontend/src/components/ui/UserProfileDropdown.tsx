/**
 * User profile dropdown with avatar, name, role badge, language selector, and logout.
 */
import { useState, useRef, useEffect } from 'react'
import { useTranslation } from 'react-i18next'
import { useAuth, useUserDisplayName } from '../../hooks/useAuth'
import { api } from '../../api/client'
import {
  SUPPORTED_LANGUAGES,
  LANGUAGE_LABELS,
  LANGUAGE_FLAGS,
  type SupportedLanguage,
} from '../../lib/i18n'
import type { UserRole } from '../../types/api'

// Role badge colors
const roleBadgeStyles: Record<UserRole, string> = {
  admin: 'bg-purple-100 text-purple-700',
  editor: 'bg-blue-100 text-blue-700',
  viewer: 'bg-slate-100 text-slate-600',
}

export function UserProfileDropdown() {
  const { t, i18n } = useTranslation()
  const { user, isAuthenticated, isLoading } = useAuth()
  const displayName = useUserDisplayName()
  const [isOpen, setIsOpen] = useState(false)
  const [imageError, setImageError] = useState(false)
  const dropdownRef = useRef<HTMLDivElement>(null)

  const currentLang = i18n.language as SupportedLanguage

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

  const handleLanguageChange = (lang: SupportedLanguage) => {
    i18n.changeLanguage(lang)
    // Fire-and-forget server sync
    api.updatePreferences({ language: lang }).catch(() => {})
  }

  // Cycle to next language
  const cycleLanguage = () => {
    const idx = SUPPORTED_LANGUAGES.indexOf(currentLang)
    const next = SUPPORTED_LANGUAGES[(idx + 1) % SUPPORTED_LANGUAGES.length]
    handleLanguageChange(next)
  }

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
        <span className="hidden sm:inline">{t('profile.login')}</span>
      </a>
    )
  }

  // Get 2-letter initials from name, username, or fallback
  const getInitials = () => {
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
  const initials = getInitials()

  const roleKey = `profile.${user.role}` as const

  return (
    <div ref={dropdownRef} className="relative">
      {/* Avatar button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-2 p-1 rounded-full hover:bg-slate-100 transition-colors focus:outline-none focus:ring-2 focus:ring-purple-500/40 focus:ring-offset-2"
        aria-expanded={isOpen}
        aria-haspopup="true"
      >
        {/* Always show initials, optionally overlay with photo */}
        <div className="relative w-10 h-10">
          {/* Initials background - always visible */}
          <div className="absolute inset-0 rounded-full bg-gradient-to-br from-purple-500 to-blue-600 flex items-center justify-center text-white font-bold text-sm shadow-sm">
            {initials}
          </div>
          {/* Photo overlay - only if available and not errored */}
          {user.photo_url && !imageError && (
            <img
              src={user.photo_url}
              alt={displayName}
              className="absolute inset-0 w-10 h-10 rounded-full object-cover border-2 border-white shadow-sm"
              onError={() => setImageError(true)}
            />
          )}
        </div>
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

      {/* Dropdown menu - opens upward */}
      {isOpen && (
        <div className="absolute left-0 bottom-full mb-2 w-64 bg-white rounded-xl shadow-lg border border-slate-200 py-2 z-50 animate-in fade-in slide-in-from-bottom-2 duration-200">
          {/* User info header */}
          <div className="px-4 py-3 border-b border-slate-100">
            <div className="flex items-center gap-3">
              {/* Avatar with initials fallback */}
              <div className="relative w-10 h-10 flex-shrink-0">
                <div className="absolute inset-0 rounded-full bg-gradient-to-br from-purple-500 to-blue-600 flex items-center justify-center text-white font-bold text-sm">
                  {initials}
                </div>
                {user.photo_url && !imageError && (
                  <img
                    src={user.photo_url}
                    alt={displayName}
                    className="absolute inset-0 w-10 h-10 rounded-full object-cover"
                    onError={() => setImageError(true)}
                  />
                )}
              </div>
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
                {t(roleKey)}
              </span>
            </div>
          </div>

          {/* Language selector */}
          <div className="py-1 border-b border-slate-100">
            <button
              onClick={cycleLanguage}
              className="flex items-center justify-between w-full px-4 py-2 text-sm text-slate-700 hover:bg-slate-50 transition-colors"
            >
              <div className="flex items-center gap-3">
                <svg className="w-4 h-4 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 5h12M9 3v2m1.048 9.5A18.022 18.022 0 016.412 9m6.088 9h7M11 21l5-10 5 10M12.751 5C11.783 10.77 8.07 15.61 3 18.129" />
                </svg>
                <span>{t('profile.language')}</span>
              </div>
              <span className="text-xs text-slate-500">
                {LANGUAGE_FLAGS[currentLang]} {LANGUAGE_LABELS[currentLang]}
              </span>
            </button>
          </div>

          {/* Menu items */}
          <div className="py-1">
            {/* Admin links (admin only) */}
            {user.role === 'admin' && (
              <>
                <a
                  href="/v2/admin/users"
                  className="flex items-center gap-3 px-4 py-2 text-sm text-slate-700 hover:bg-slate-50 transition-colors"
                  onClick={() => setIsOpen(false)}
                >
                  <svg className="w-4 h-4 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4.354a4 4 0 110 5.292M15 21H3v-1a6 6 0 0112 0v1zm0 0h6v-1a6 6 0 00-9-5.197M13 7a4 4 0 11-8 0 4 4 0 018 0z" />
                  </svg>
                  {t('profile.manageUsers')}
                </a>
                <a
                  href="/v2/admin/permissions"
                  className="flex items-center gap-3 px-4 py-2 text-sm text-slate-700 hover:bg-slate-50 transition-colors"
                  onClick={() => setIsOpen(false)}
                >
                  <svg className="w-4 h-4 text-slate-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m5.618-4.016A11.955 11.955 0 0112 2.944a11.955 11.955 0 01-8.618 3.04A12.02 12.02 0 003 9c0 5.591 3.824 10.29 9 11.622 5.176-1.332 9-6.03 9-11.622 0-1.042-.133-2.052-.382-3.016z" />
                  </svg>
                  {t('profile.permissions')}
                </a>
              </>
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
              {t('profile.logout')}
            </a>
          </div>
        </div>
      )}
    </div>
  )
}
