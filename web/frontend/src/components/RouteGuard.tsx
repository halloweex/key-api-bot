import { memo, type ReactNode } from 'react'
import { Spinner } from './Spinner'
import { useAuth, usePermission } from '../hooks/useAuth'
import { navigate } from '../hooks/useRouter'
import type { Permissions } from '../types/api'

// ─── Route guards ────────────────────────────────────────────────────────────
//
// Wrappers that keep a route's content off-screen until the caller is allowed
// to see it: a full-screen spinner while auth resolves, a redirect to "/" when
// it resolves against them. Two flavours share one shape:
//
//   <AdminGuard>       — requires the admin role
//   <PermissionGuard>  — requires view access to one feature. SMS campaigns
//                        are grantable on their own (the `marketer` role), so
//                        that page must not ask "are you an admin" — the
//                        server does not either.

function GuardedContent({
  isLoading,
  allowed,
  children,
}: {
  isLoading: boolean
  allowed: boolean
  children: ReactNode
}) {
  if (isLoading) {
    return <Spinner variant="screen" />
  }

  if (!allowed) {
    navigate('/')
    return null
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
