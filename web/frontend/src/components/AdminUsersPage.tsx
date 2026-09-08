/**
 * Admin Users Management Page
 *
 * Allows admins to view all users, change roles, and manage access.
 */
import { useState } from 'react'
import { ShieldCheck, ArrowLeft } from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../api/client'
import type { TabFeature, UserRole, UserStatus } from '../types/api'
import { Card, CardHeader, CardTitle, CardContent } from './Card'
import { Select } from './Select'
import { SkeletonTable } from './Skeleton'
import { ApiErrorState } from './ApiErrorState'
import { EmptyState } from './EmptyState'
import { DataTable, Tr, Th } from './DataTable'
import { PageHeaderLink } from './PageHeaderLink'
import { PageShell } from './PageShell'
import { PageHeading } from './PageHeading'
import { Wrapper } from './Wrapper'
import { UserRow, roleOptions, statusOptions } from './UserRow'
import { AccessRequestsCard } from './AccessRequestsCard'

export function AdminUsersPage() {
  const queryClient = useQueryClient()
  const [statusFilter, setStatusFilter] = useState<string | null>(null)
  const [roleFilter, setRoleFilter] = useState<string | null>(null)
  const [updatingUsers, setUpdatingUsers] = useState<Set<number>>(new Set())
  const [expanded, setExpanded] = useState<Set<number>>(new Set())

  // The grantable tabs and the ready-made bundles, from the server rather than
  // from a copy compiled into the page: a tab added in `core/permissions.py`
  // has to appear here without a frontend release, or the checklist and the
  // bot's keyboard drift apart.
  const { data: matrix } = useQuery({
    // Same key the permissions page uses: one endpoint, one cache entry —
    // two would go stale independently of each other.
    queryKey: ['adminPermissions'],
    queryFn: () => api.getPermissionsMatrix(),
    staleTime: 10 * 60 * 1000,
  })

  const { data, isLoading, error, refetch } = useQuery({
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

  const updateFeaturesMutation = useMutation({
    mutationFn: ({ userId, features, preset }: {
      userId: number
      features?: TabFeature[] | null
      preset?: string
    }) =>
      preset !== undefined
        ? api.applyUserPreset(userId, preset)
        : api.updateUserFeatures(userId, features ?? null),
    onMutate: ({ userId }) => {
      setUpdatingUsers((prev) => new Set(prev).add(userId))
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['adminUsers'] })
      // The admin may be editing their own row, and the sidebar reads the same
      // answer — without this the nav keeps the tabs it drew a minute ago.
      queryClient.invalidateQueries({ queryKey: ['currentUser'] })
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

  const handleFeaturesChange = (userId: number, features: TabFeature[] | null) => {
    updateFeaturesMutation.mutate({ userId, features })
  }

  const handlePreset = (userId: number, preset: string) => {
    updateFeaturesMutation.mutate({ userId, preset })
  }

  const handleToggleExpanded = (userId: number) => {
    setExpanded((prev) => {
      const next = new Set(prev)
      if (next.has(userId)) next.delete(userId)
      else next.add(userId)
      return next
    })
  }

  const handleStatusChange = (userId: number, status: UserStatus) => {
    updateStatusMutation.mutate({ userId, status })
  }

  const users = data?.users ?? []

  return (
    <PageShell variant="admin">
      <PageHeading
        title="User Management"
        subtitle="Manage user roles and access permissions"
        actions={
          <>
            <PageHeaderLink href="/admin/permissions" icon={ShieldCheck}>
              Permissions
            </PageHeaderLink>
            <PageHeaderLink href="/" icon={ArrowLeft}>
              Dashboard
            </PageHeaderLink>
          </>
        }
      />

      {/* Anyone waiting to be let in, above the list of people already in. */}
      <AccessRequestsCard presets={matrix?.presets} tabs={matrix?.tabs} />

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
            <SkeletonTable />
          ) : error ? (
            <ApiErrorState
              error={error as Error}
              onRetry={refetch}
              title="Failed to load users"
            />
          ) : users.length === 0 ? (
            <EmptyState message="No users found" />
          ) : (
            <DataTable variant="admin" stickyHeader>
              <thead className="sticky top-0 z-10 bg-white">
                <Tr header variant="admin" sticky>
                  <Th variant="admin" sticky>User</Th>
                  <Th variant="admin" sticky>Role</Th>
                  <Th variant="admin" sticky>Status</Th>
                  <Th variant="admin" sticky>Tabs</Th>
                  <Th variant="admin" sticky>Last Activity</Th>
                </Tr>
              </thead>
              <tbody>
                {users.map((user) => (
                  <UserRow
                    key={user.user_id}
                    user={user}
                    onRoleChange={handleRoleChange}
                    onStatusChange={handleStatusChange}
                    onFeaturesChange={handleFeaturesChange}
                    onPreset={handlePreset}
                    expanded={expanded.has(user.user_id)}
                    onToggleExpanded={handleToggleExpanded}
                    tabs={matrix?.tabs}
                    presets={matrix?.presets}
                    isUpdating={updatingUsers.has(user.user_id)}
                  />
                ))}
              </tbody>
            </DataTable>
          )}
        </CardContent>
      </Card>
    </PageShell>
  )
}
