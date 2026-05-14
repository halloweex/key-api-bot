import { memo } from 'react'
import type { AdminUser, UserRole, UserStatus } from '../types/api'
import { BadgeSelect } from './BadgeSelect'
import { Tr, Td } from './DataTable'

type Tone = 'purple' | 'blue' | 'slate' | 'green' | 'yellow' | 'red'

const roleTone: Record<UserRole, Tone> = {
  admin: 'purple',
  editor: 'blue',
  viewer: 'slate',
}

const statusTone: Record<UserStatus, Tone> = {
  approved: 'green',
  pending: 'yellow',
  denied: 'red',
  frozen: 'slate',
}

export const roleOptions = [
  { value: 'admin', label: 'Admin' },
  { value: 'editor', label: 'Editor' },
  { value: 'viewer', label: 'Viewer' },
]

export const statusOptions = [
  { value: 'approved', label: 'Approved' },
  { value: 'pending', label: 'Pending' },
  { value: 'denied', label: 'Denied' },
  { value: 'frozen', label: 'Frozen' },
]

interface UserRowProps {
  user: AdminUser
  onRoleChange: (userId: number, role: UserRole) => void
  onStatusChange: (userId: number, status: UserStatus) => void
  isUpdating: boolean
}

export const UserRow = memo(function UserRow({
  user,
  onRoleChange,
  onStatusChange,
  isUpdating,
}: UserRowProps) {
  const displayName = user.first_name
    ? `${user.first_name}${user.last_name ? ` ${user.last_name}` : ''}`
    : user.username
      ? `@${user.username}`
      : `User ${user.user_id}`

  const initials = user.first_name
    ? user.first_name.charAt(0).toUpperCase()
    : user.username
      ? user.username.charAt(0).toUpperCase()
      : '?'

  const lastActivity = user.last_activity
    ? new Date(user.last_activity).toLocaleDateString()
    : 'Never'

  return (
    <Tr variant="admin" faded={isUpdating}>
      <Td variant="admin">
        <div className="flex items-center gap-3">
          {user.photo_url ? (
            <img
              src={user.photo_url}
              alt={displayName}
              className="w-9 h-9 rounded-full object-cover"
            />
          ) : (
            <div className="w-9 h-9 rounded-full bg-gradient-to-br from-purple-500 to-blue-600 flex items-center justify-center text-white font-semibold text-sm">
              {initials}
            </div>
          )}
          <div>
            <p className="font-medium text-slate-900">{displayName}</p>
            <p className="text-xs text-slate-500">ID: {user.user_id}</p>
          </div>
        </div>
      </Td>

      <Td variant="admin">
        <BadgeSelect
          tone={roleTone[user.role]}
          options={roleOptions}
          value={user.role}
          onChange={(v) => onRoleChange(user.user_id, v as UserRole)}
          disabled={isUpdating}
        />
      </Td>

      <Td variant="admin">
        <BadgeSelect
          tone={statusTone[user.status]}
          options={statusOptions}
          value={user.status}
          onChange={(v) => onStatusChange(user.user_id, v as UserStatus)}
          disabled={isUpdating}
        />
      </Td>

      <Td variant="admin">
        <span className="text-sm text-slate-600">{lastActivity}</span>
      </Td>
    </Tr>
  )
})
