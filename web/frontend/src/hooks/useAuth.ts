/**
 * Authentication hook for user data and permissions.
 */
import { useEffect } from 'react'
import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'
import i18n, { SUPPORTED_LANGUAGES, type SupportedLanguage } from '../lib/i18n'
import type { CurrentUserResponse, Permissions, UserRole } from '../types/api'

// Cache TTL: 5 minutes (user data changes rarely)
const AUTH_CACHE_TTL = 5 * 60 * 1000

// Query key for auth data
export const authQueryKey = ['currentUser'] as const

/**
 * Fetch and cache current user data.
 *
 * Returns user info and permissions from /api/me endpoint.
 * Handles 401 gracefully (user not authenticated).
 */
export function useAuth() {
  const query = useQuery<CurrentUserResponse | null>({
    queryKey: authQueryKey,
    queryFn: async () => {
      try {
        return await api.getCurrentUser()
      } catch (error) {
        // 401 means not authenticated - return null instead of throwing
        if (error instanceof Error && 'status' in error && (error as any).status === 401) {
          return null
        }
        throw error
      }
    },
    staleTime: AUTH_CACHE_TTL,
    // Don't refetch on window focus for auth - user must explicitly re-login
    refetchOnWindowFocus: false,
    // Retry only for server errors, not auth errors
    retry: (failureCount, error) => {
      if (error instanceof Error && 'status' in error) {
        const status = (error as any).status
        // Don't retry auth errors (401, 403)
        if (status === 401 || status === 403) return false
      }
      return failureCount < 2
    },
  })

  const user = query.data?.user ?? null
  const permissions = query.data?.permissions ?? null
  const preferences = query.data?.preferences ?? null
  const isAuthenticated = !!user
  const isLoading = query.isLoading
  const error = query.error

  // Sync language from server preferences (server wins for cross-device consistency)
  useEffect(() => {
    if (preferences?.language) {
      const serverLang = preferences.language as SupportedLanguage
      if (SUPPORTED_LANGUAGES.includes(serverLang) && i18n.language !== serverLang) {
        i18n.changeLanguage(serverLang)
      }
    }
  }, [preferences?.language])

  return {
    user,
    permissions,
    preferences,
    isAuthenticated,
    isLoading,
    error,
    refetch: query.refetch,
  }
}

/**
 * Hook to check permissions for a specific feature.
 *
 * Usage:
 *   const { canView, canEdit, canDelete } = usePermission('expenses')
 */
export function usePermission(feature: keyof Permissions) {
  const { permissions, isLoading } = useAuth()

  const featurePerms = permissions?.[feature]

  return {
    canView: featurePerms?.view ?? false,
    canEdit: featurePerms?.edit ?? false,
    canDelete: featurePerms?.delete ?? false,
    isLoading,
  }
}

/**
 * Hook to check if user has a specific role.
 *
 * Usage:
 *   const isAdmin = useRole('admin')
 */
export function useRole(role: UserRole): boolean {
  const { user } = useAuth()
  return user?.role === role
}

/**
 * Hook to check if user is admin.
 *
 * Usage:
 *   const isAdmin = useIsAdmin()
 */
export function useIsAdmin(): boolean {
  return useRole('admin')
}

/**
 * Get user display name.
 */
export function useUserDisplayName(): string {
  const { user } = useAuth()
  if (!user) return ''

  if (user.first_name && user.last_name) {
    return `${user.first_name} ${user.last_name}`
  }
  if (user.first_name) return user.first_name
  if (user.username) return `@${user.username}`
  return `User ${user.id}`
}
