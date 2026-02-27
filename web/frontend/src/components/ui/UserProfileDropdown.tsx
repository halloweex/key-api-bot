/**
 * User profile dropdown with avatar, name, role badge, language selector, and logout.
 */
import { useState, useRef, useEffect } from 'react'
import { useTranslation } from 'react-i18next'
import { LogIn, LogOut, ChevronDown, Globe, Check, Users, ShieldCheck } from 'lucide-react'
import { useAuth, useUserDisplayName } from '../../hooks/useAuth'
import { api } from '../../api/client'
import { UserAvatar } from './UserAvatar'
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
  const [langOpen, setLangOpen] = useState(false)
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
        <LogIn className="w-5 h-5" />
        <span className="hidden sm:inline">{t('profile.login')}</span>
      </a>
    )
  }

  const avatarName = displayName || user.username || 'User'
  const roleKey = `profile.${user.role}` as const

  return (
    <div ref={dropdownRef} className="relative">
      {/* Avatar button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        className="flex items-center gap-2.5 w-full p-1.5 rounded-lg hover:bg-slate-100 transition-colors focus:outline-none focus:ring-2 focus:ring-purple-500/40 focus:ring-offset-2"
        aria-expanded={isOpen}
        aria-haspopup="true"
      >
        <UserAvatar
          name={avatarName}
          photoUrl={user.photo_url}
          size={36}
        />
        {/* Name and username */}
        <div className="min-w-0 flex-1 text-left">
          <p className="text-sm font-medium text-slate-800 truncate">
            {displayName}
          </p>
          {user.username && (
            <p className="text-[11px] text-slate-500 truncate">
              @{user.username}
            </p>
          )}
        </div>
        {/* Chevron */}
        <ChevronDown
          className={`w-4 h-4 text-slate-400 transition-transform flex-shrink-0 ${isOpen ? 'rotate-180' : ''}`}
        />
      </button>

      {/* Dropdown menu - opens upward */}
      {isOpen && (
        <div className="absolute left-0 bottom-full mb-2 w-64 bg-white rounded-xl shadow-lg border border-slate-200 py-2 z-50 animate-in fade-in slide-in-from-bottom-2 duration-200">
          {/* User info header */}
          <div className="px-4 py-3 border-b border-slate-100">
            <div className="flex items-center gap-3">
              <UserAvatar
                name={avatarName}
                photoUrl={user.photo_url}
                size={40}
              />
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

          {/* Language selector (accordion) */}
          <div className="py-1 border-b border-slate-100">
            <button
              onClick={() => setLangOpen(!langOpen)}
              className="flex items-center justify-between w-full px-4 py-2 text-sm text-slate-700 hover:bg-slate-50 transition-colors"
            >
              <div className="flex items-center gap-3">
                <Globe className="w-4 h-4 text-slate-400" />
                <span>{t('profile.language')}</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="text-xs text-slate-500">{LANGUAGE_FLAGS[currentLang]} {LANGUAGE_LABELS[currentLang]}</span>
                <ChevronDown className={`w-3.5 h-3.5 text-slate-400 transition-transform ${langOpen ? 'rotate-180' : ''}`} />
              </div>
            </button>
            {langOpen && (
              <div className="pb-1">
                {SUPPORTED_LANGUAGES.map((lang) => (
                  <button
                    key={lang}
                    onClick={() => handleLanguageChange(lang)}
                    className={`flex items-center justify-between w-full pl-11 pr-4 py-1.5 text-sm transition-colors ${
                      currentLang === lang
                        ? 'text-purple-700 bg-purple-50'
                        : 'text-slate-600 hover:bg-slate-50'
                    }`}
                  >
                    <div className="flex items-center gap-2.5">
                      <span>{LANGUAGE_FLAGS[lang]}</span>
                      <span className={currentLang === lang ? 'font-medium' : ''}>{LANGUAGE_LABELS[lang]}</span>
                    </div>
                    {currentLang === lang && (
                      <Check className="w-4 h-4 text-purple-600" strokeWidth={2.5} />
                    )}
                  </button>
                ))}
              </div>
            )}
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
                  <Users className="w-4 h-4 text-slate-400" />
                  {t('profile.manageUsers')}
                </a>
                <a
                  href="/v2/admin/permissions"
                  className="flex items-center gap-3 px-4 py-2 text-sm text-slate-700 hover:bg-slate-50 transition-colors"
                  onClick={() => setIsOpen(false)}
                >
                  <ShieldCheck className="w-4 h-4 text-slate-400" />
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
              <LogOut className="w-4 h-4" />
              {t('profile.logout')}
            </a>
          </div>
        </div>
      )}
    </div>
  )
}
