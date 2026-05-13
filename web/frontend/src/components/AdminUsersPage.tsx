/**
 * Admin Users Management Page
 *
 * Allows admins to view all users, change roles, and manage access.
 */
import { useState } from 'react'
import { ShieldCheck, ArrowLeft } from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../api/client'
import type { UserRole, UserStatus } from '../types/api'
import { Card, CardHeader, CardTitle, CardContent } from './Card'
import { Select } from './Select'
import { Wrapper } from './Wrapper'
import { UserRow, roleOptions, statusOptions } from './UserRow'

export function AdminUsersPage() {
  const queryClient = useQueryClient()
  const [statusFilter, setStatusFilter] = useState<string | null>(null)
  const [roleFilter, setRoleFilter] = useState<string | null>(null)
  const [updatingUsers, setUpdatingUsers] = useState<Set<number>>(new Set())

  const { data, isLoading, error } = useQuery({
    queryKey: ['adminUsers', statusFilter, roleFilter],
    queryFn: () => api.getAdminUsers(
      statusFilter as UserStatus | undefined,
      roleFilter as UserRole | undefined,
    ),
    staleTime: 60 * 1000,
  })

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
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">User Management</h1>
          <p className="text-sm text-slate-500 mt-1">
            Manage user roles and access permissions
          </p>
        </div>
        <div className="flex gap-3">
          <a
            href="/admin/permissions"
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors"
          >
            <ShieldCheck className="w-4 h-4" />
            Permissions
          </a>
          <a
            href="/"
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors"
          >
            <ArrowLeft className="w-4 h-4" />
            Dashboard
          </a>
        </div>
      </div>

      <Card>
        <CardHeader>
          <Wrapper dir="row-responsive" align="center" justify="between" gap="lg">
            <CardTitle>Users ({users.length})</CardTitle>
            <Wrapper dir="row" gap="md">
              <Select
                options={statusOptions}
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
            </Wrapper>
          </Wrapper>
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
            <div className="overflow-x-auto max-h-[70vh] overflow-y-auto">
              <table className="w-full">
                <thead className="sticky top-0 z-10 bg-white">
                  <tr className="border-b border-slate-200 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    <th className="py-3 px-4 bg-white">User</th>
                    <th className="py-3 px-4 bg-white">Role</th>
                    <th className="py-3 px-4 bg-white">Status</th>
                    <th className="py-3 px-4 bg-white">Last Activity</th>
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
