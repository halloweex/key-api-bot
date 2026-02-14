/**
 * Admin Users Management Page
 *
 * Allows admins to view all users, change roles, and manage access.
 */
import { useState, memo } from 'react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../../api/client'
import type { AdminUser, UserRole, UserStatus } from '../../types/api'
import { Card, CardHeader, CardTitle, CardContent } from '../ui/Card'
import { Select } from '../ui/Select'

// Role badge styles
const roleBadgeStyles: Record<UserRole, string> = {
  admin: 'bg-purple-100 text-purple-700 border-purple-200',
  editor: 'bg-blue-100 text-blue-700 border-blue-200',
  viewer: 'bg-slate-100 text-slate-600 border-slate-200',
}

// Status badge styles
const statusBadgeStyles: Record<UserStatus, string> = {
  approved: 'bg-green-100 text-green-700 border-green-200',
  pending: 'bg-yellow-100 text-yellow-700 border-yellow-200',
  denied: 'bg-red-100 text-red-700 border-red-200',
  frozen: 'bg-slate-100 text-slate-600 border-slate-200',
}

const roleOptions = [
  { value: 'admin', label: 'Admin' },
  { value: 'editor', label: 'Editor' },
  { value: 'viewer', label: 'Viewer' },
]

const statusOptions = [
  { value: 'approved', label: 'Approved' },
  { value: 'pending', label: 'Pending' },
  { value: 'denied', label: 'Denied' },
  { value: 'frozen', label: 'Frozen' },
]

// User row component
const UserRow = memo(function UserRow({
  user,
  onRoleChange,
  onStatusChange,
  isUpdating,
}: {
  user: AdminUser
  onRoleChange: (userId: number, role: UserRole) => void
  onStatusChange: (userId: number, status: UserStatus) => void
  isUpdating: boolean
}) {
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
      {/* User info */}
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

      {/* Role */}
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

      {/* Status */}
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

      {/* Last Activity */}
      <td className="py-3 px-4 text-sm text-slate-600">
        {lastActivity}
      </td>
    </tr>
  )
})

export function AdminUsersPage() {
  const queryClient = useQueryClient()
  const [statusFilter, setStatusFilter] = useState<string | null>(null)
  const [roleFilter, setRoleFilter] = useState<string | null>(null)
  const [updatingUsers, setUpdatingUsers] = useState<Set<number>>(new Set())

  // Fetch users
  const { data, isLoading, error } = useQuery({
    queryKey: ['adminUsers', statusFilter, roleFilter],
    queryFn: () => api.getAdminUsers(
      statusFilter as UserStatus | undefined,
      roleFilter as UserRole | undefined,
    ),
    staleTime: 60 * 1000, // 1 minute
  })

  // Update role mutation
  const updateRoleMutation = useMutation({
    mutationFn: ({ userId, role }: { userId: number; role: UserRole }) =>
      api.updateUserRole(userId, role),
    onMutate: ({ userId }) => {
      setUpdatingUsers((prev) => new Set(prev).add(userId))
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['adminUsers'] })
    },
    onSettled: (_, __, { userId }) => {
      setUpdatingUsers((prev) => {
        const next = new Set(prev)
        next.delete(userId)
        return next
      })
    },
  })

  // Update status mutation
  const updateStatusMutation = useMutation({
    mutationFn: ({ userId, status }: { userId: number; status: UserStatus }) =>
      api.updateUserStatus(userId, status),
    onMutate: ({ userId }) => {
      setUpdatingUsers((prev) => new Set(prev).add(userId))
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['adminUsers'] })
    },
    onSettled: (_, __, { userId }) => {
      setUpdatingUsers((prev) => {
        const next = new Set(prev)
        next.delete(userId)
        return next
      })
    },
  })

  const handleRoleChange = (userId: number, role: UserRole) => {
    updateRoleMutation.mutate({ userId, role })
  }

  const handleStatusChange = (userId: number, status: UserStatus) => {
    updateStatusMutation.mutate({ userId, status })
  }

  const users = data?.users ?? []

  return (
    <main className="p-3 sm:p-6 lg:p-8 max-w-[1400px] mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">User Management</h1>
          <p className="text-sm text-slate-500 mt-1">
            Manage user roles and access permissions
          </p>
        </div>
        <a
          href="/v2"
          className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors"
        >
          <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 19l-7-7m0 0l7-7m-7 7h18" />
          </svg>
          Back to Dashboard
        </a>
      </div>

      <Card>
        <CardHeader>
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <CardTitle>Users ({users.length})</CardTitle>
            <div className="flex gap-3">
              <Select
                options={[
                  { value: 'approved', label: 'Approved' },
                  { value: 'pending', label: 'Pending' },
                  { value: 'denied', label: 'Denied' },
                  { value: 'frozen', label: 'Frozen' },
                ]}
                value={statusFilter}
                onChange={setStatusFilter}
                placeholder="All Statuses"
                allowEmpty={true}
                emptyLabel="All Statuses"
              />
              <Select
                options={roleOptions}
                value={roleFilter}
                onChange={setRoleFilter}
                placeholder="All Roles"
                allowEmpty={true}
                emptyLabel="All Roles"
              />
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {isLoading ? (
            <div className="py-12 text-center text-slate-500">
              Loading users...
            </div>
          ) : error ? (
            <div className="py-12 text-center text-red-500">
              Error loading users. Please try again.
            </div>
          ) : users.length === 0 ? (
            <div className="py-12 text-center text-slate-500">
              No users found.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-slate-200 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    <th className="py-3 px-4">User</th>
                    <th className="py-3 px-4">Role</th>
                    <th className="py-3 px-4">Status</th>
                    <th className="py-3 px-4">Last Activity</th>
                  </tr>
                </thead>
                <tbody>
                  {users.map((user) => (
                    <UserRow
                      key={user.user_id}
                      user={user}
                      onRoleChange={handleRoleChange}
                      onStatusChange={handleStatusChange}
                      isUpdating={updatingUsers.has(user.user_id)}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>
    </main>
  )
}
