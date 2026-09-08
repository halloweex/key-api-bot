/**
 * Admin Users Management Page
 *
 * Allows admins to view all users, change roles, and manage access.
 */
import { useState } from 'react'
import { ShieldCheck, ArrowLeft } from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../api/client'
import { useAuth } from '../hooks/useAuth'
import type { AdminUsersResponse, TabFeature, UserRole, UserStatus } from '../types/api'
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
import { defaultTabsFor } from '../utils/access'

export function AdminUsersPage() {
  const queryClient = useQueryClient()
  const { user: currentUser } = useAuth()
  const currentUserId = currentUser?.id
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

  // ── Tabs: optimistic, because a checkbox has to move when it is clicked ──
  //
  // It used to cost three sequential round trips before the tick appeared —
  // PATCH, then a refetch of the whole user list, then /api/me — and the chips
  // were disabled for all three, because the chip's state is read from the
  // list query. The database is not the reason it felt slow: measured on
  // production, the write is 1.95 ms and the list read 1.58 ms. It was the
  // waiting, and the control being dead while it waited.
  //
  // So the cache is written first and the request follows. `onError` puts back
  // exactly what was there, `onSettled` refetches the truth either way, and
  // /api/me is invalidated only when the admin is editing their own row —
  // which is the only case where the sidebar's own answer changed.
  const patchCachedFeatures = (userId: number, features: TabFeature[] | null) => {
    const previous = queryClient.getQueriesData<AdminUsersResponse>({
      queryKey: ['adminUsers'],
    })
    queryClient.setQueriesData<AdminUsersResponse>(
      { queryKey: ['adminUsers'] },
      (old) =>
        old
          ? {
              ...old,
              users: old.users.map((u) =>
                u.user_id === userId ? { ...u, allowed_features: features } : u,
              ),
            }
          : old,
    )
    return previous
  }

  const updateFeaturesMutation = useMutation({
    mutationFn: ({ userId, features, preset }: {
      userId: number
      features?: TabFeature[] | null
      preset?: string
    }) =>
      preset !== undefined
        ? api.applyUserPreset(userId, preset)
        : api.updateUserFeatures(userId, features ?? null),
    onMutate: async ({ userId, features, preset }) => {
      // Stop an in-flight refetch from landing on top of the optimistic write.
      await queryClient.cancelQueries({ queryKey: ['adminUsers'] })
      const next = preset !== undefined
        ? (matrix?.presets?.find((p) => p.key === preset)?.features ?? null)
        : (features ?? null)
      return { previous: patchCachedFeatures(userId, next) }
    },
    onError: (_error, _variables, context) => {
      context?.previous?.forEach(([key, data]) => {
        queryClient.setQueryData(key, data)
      })
    },
    onSettled: (_data, _error, { userId }) => {
      queryClient.invalidateQueries({ queryKey: ['adminUsers'] })
      if (userId === currentUserId) {
        queryClient.invalidateQueries({ queryKey: ['currentUser'] })
      }
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
                    roleFeatures={defaultTabsFor(matrix?.default_tabs, user.role)}
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
