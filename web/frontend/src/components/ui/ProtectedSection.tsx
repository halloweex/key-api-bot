/**
 * ProtectedSection - Conditionally renders children based on permissions.
 *
 * Usage:
 *   <ProtectedSection feature="expenses">
 *     <ManualExpensesTable />
 *   </ProtectedSection>
 */
import { type ReactNode, memo } from 'react'
import { usePermission } from '../../hooks/useAuth'
import type { Permissions } from '../../types/api'

interface ProtectedSectionProps {
  /** Feature to check permission for */
  feature: keyof Permissions
  /** Action to check (default: view) */
  action?: 'view' | 'edit' | 'delete'
  /** Children to render if permitted */
  children: ReactNode
  /** Optional fallback to show when not permitted (default: null) */
  fallback?: ReactNode
}

export const ProtectedSection = memo(function ProtectedSection({
  feature,
  action = 'view',
  children,
  fallback = null,
}: ProtectedSectionProps) {
  const { canView, canEdit, canDelete, isLoading } = usePermission(feature)

  // While loading, render nothing to avoid flash
  if (isLoading) {
    return null
  }

  // Check the requested permission
  const hasPermission =
    action === 'view' ? canView :
    action === 'edit' ? canEdit :
    action === 'delete' ? canDelete :
    false

  if (!hasPermission) {
    return <>{fallback}</>
  }

  return <>{children}</>
})
