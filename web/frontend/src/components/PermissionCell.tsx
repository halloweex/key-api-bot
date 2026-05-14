import { memo, useCallback } from 'react'
import type { UserRole, FeaturePermissions } from '../types/api'
import { PermissionCheckbox } from './PermissionCheckbox'
import { Td } from './DataTable'
import { Wrapper } from './Wrapper'

export const actions = ['view', 'edit', 'delete'] as const
export type Action = typeof actions[number]

export const actionLabels: Record<Action, string> = {
  view: 'View',
  edit: 'Edit',
  delete: 'Delete',
}

interface PermissionCellProps {
  role: UserRole
  featureKey: string
  permissions: FeaturePermissions
  onUpdate: (role: UserRole, feature: string, action: Action, value: boolean) => void
  isUpdating: boolean
}

export const PermissionCell = memo(function PermissionCell({
  role,
  featureKey,
  permissions,
  onUpdate,
  isUpdating,
}: PermissionCellProps) {
  const handleToggle = useCallback(
    (action: Action) => {
      const newValue = !permissions[action]
      onUpdate(role, featureKey, action, newValue)
    },
    [role, featureKey, permissions, onUpdate]
  )

  return (
    <Td variant="admin" faded={isUpdating}>
      <Wrapper dir="row" gap="md" wrap>
        {actions.map((action) => (
          <PermissionCheckbox
            key={action}
            checked={permissions[action]}
            onChange={() => handleToggle(action)}
            disabled={isUpdating}
            label={actionLabels[action]}
          />
        ))}
      </Wrapper>
    </Td>
  )
})
