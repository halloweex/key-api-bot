import { memo, useEffect, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { Lock } from 'lucide-react'
import { Spinner } from './Spinner'
import { useAuth, useFirstAllowedPath, usePermission } from '../hooks/useAuth'
import { navigate, useRouter } from '../hooks/useRouter'
import type { Permissions } from '../types/api'

// ─── Route guards ────────────────────────────────────────────────────────────
//
// Wrappers that keep a route's content off-screen until the caller is allowed
// to see it: a full-screen spinner while auth resolves, and — when it resolves
// against them — the first page they *can* open. Two flavours share one shape:
//
//   <AdminGuard>       — requires the admin role
//   <PermissionGuard>  — requires view access to one feature
//
// Every tab is a permission now, so `PermissionGuard` wraps all of them and
// `AdminGuard` is left to the two admin pages, which are gated on the role and
// deliberately not on a checkbox.
//
// **The redirect cannot be a constant.** It used to be `navigate('/')`, which
// was safe only while everybody could open the dashboard. An account granted
// /traffic alone is denied at `/`, and sending it to `/` would be a redirect
// loop — the browser tab spinning on a permission, which is the worst possible
// way to say "you do not have this". So the fallback is computed from what the
// account may actually see, and when that is nothing at all it is a sentence
// on the screen rather than a navigation.

export const NoAccessNotice = memo(function NoAccessNotice() {
  const { t } = useTranslation()
  return (
    <div className="min-h-screen bg-slate-50 flex items-center justify-center p-6">
      <div className="max-w-sm text-center">
        <div className="mx-auto mb-4 w-12 h-12 rounded-full bg-slate-200 flex items-center justify-center">
          <Lock className="w-6 h-6 text-slate-500" aria-hidden />
        </div>
        <h1 className="text-base font-semibold text-slate-900">
          {t('access.noneTitle')}
        </h1>
        <p className="mt-1 text-sm text-slate-500">{t('access.noneBody')}</p>
      </div>
    </div>
  )
})

function GuardedContent({
  isLoading,
  allowed,
  children,
}: {
  isLoading: boolean
  allowed: boolean
  children: ReactNode
}) {
  const path = useRouter()
  const fallback = useFirstAllowedPath()

  // In an effect, not during render: navigating while React is rendering is
  // how the previous version could re-enter this component before the URL had
  // changed. The dependency list ends the loop by construction — a fallback
  // equal to the page we are already on is not navigated to.
  useEffect(() => {
    if (isLoading || allowed) return
    if (fallback && fallback !== path) navigate(fallback)
  }, [isLoading, allowed, fallback, path])

  if (isLoading) {
    return <Spinner variant="screen" />
  }

  if (!allowed) {
    // Either the redirect above is about to happen, or there is nowhere to go.
    return fallback && fallback !== path ? null : <NoAccessNotice />
  }

  return <div className="min-h-screen bg-slate-50">{children}</div>
}

export const AdminGuard = memo(function AdminGuard({ children }: { children: ReactNode }) {
  const { user, isLoading } = useAuth()
  return (
    <GuardedContent isLoading={isLoading} allowed={user?.role === 'admin'}>
      {children}
    </GuardedContent>
  )
})

export const PermissionGuard = memo(function PermissionGuard({
  feature,
  children,
}: {
  feature: keyof Permissions
  children: ReactNode
}) {
  const { canView, isLoading } = usePermission(feature)
  return (
    <GuardedContent isLoading={isLoading} allowed={canView}>
      {children}
    </GuardedContent>
  )
})
