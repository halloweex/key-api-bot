import { memo } from 'react'
import type { UserRole, FeaturePermissions } from '../types/api'
import { PermissionCell, type Action } from './PermissionCell'

interface FeatureRowProps {
  featureKey: string
  featureName: string
  featureDescription: string
  permissions: Record<UserRole, FeaturePermissions>
  roles: UserRole[]
  onUpdate: (role: UserRole, feature: string, action: Action, value: boolean) => void
  updatingCells: Set<string>
}

export const FeatureRow = memo(function FeatureRow({
  featureKey,
  featureName,
  featureDescription,
  permissions,
  roles,
  onUpdate,
  updatingCells,
}: FeatureRowProps) {
  return (
    <tr className="hover:bg-slate-50/50 transition-colors">
      <td className="py-3 px-4 border-b border-slate-100">
        <div>
          <p className="font-medium text-slate-900">{featureName}</p>
          <p className="text-xs text-slate-500 mt-0.5">{featureDescription}</p>
        </div>
      </td>
      {roles.map((role) => (
        <PermissionCell
          key={role}
          role={role}
          featureKey={featureKey}
          permissions={permissions[role] || { view: false, edit: false, delete: false }}
          onUpdate={onUpdate}
          isUpdating={updatingCells.has(`${role}:${featureKey}`)}
        />
      ))}
    </tr>
  )
})
