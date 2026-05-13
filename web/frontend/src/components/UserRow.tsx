import { memo } from 'react'
import type { AdminUser, UserRole, UserStatus } from '../types/api'

const roleBadgeStyles: Record<UserRole, string> = {
  admin: 'bg-purple-100 text-purple-700 border-purple-200',
  editor: 'bg-blue-100 text-blue-700 border-blue-200',
  viewer: 'bg-slate-100 text-slate-600 border-slate-200',
}

const statusBadgeStyles: Record<UserStatus, string> = {
  approved: 'bg-green-100 text-green-700 border-green-200',
  pending: 'bg-yellow-100 text-yellow-700 border-yellow-200',
  denied: 'bg-red-100 text-red-700 border-red-200',
  frozen: 'bg-slate-100 text-slate-600 border-slate-200',
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
    <tr className={`border-b border-slate-100 hover:bg-slate-50/50 transition-colors ${isUpdating ? 'opacity-50' : ''}`}>
      <td className="py-3 px-4">
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
      </td>

      <td className="py-3 px-4">
        <select
          value={user.role}
          onChange={(e) => onRoleChange(user.user_id, e.target.value as UserRole)}
          disabled={isUpdating}
          className={`px-2 py-1 text-xs font-medium rounded-md border cursor-pointer ${roleBadgeStyles[user.role]}`}
        >
          {roleOptions.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>
      </td>

      <td className="py-3 px-4">
        <select
          value={user.status}
          onChange={(e) => onStatusChange(user.user_id, e.target.value as UserStatus)}
          disabled={isUpdating}
          className={`px-2 py-1 text-xs font-medium rounded-md border cursor-pointer ${statusBadgeStyles[user.status]}`}
        >
          {statusOptions.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>
      </td>

      <td className="py-3 px-4 text-sm text-slate-600">
        {lastActivity}
      </td>
    </tr>
  )
})
