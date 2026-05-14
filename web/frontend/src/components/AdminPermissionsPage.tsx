/**
 * Admin Permissions Matrix Page
 *
 * Allows admins to view and edit the permissions matrix for all roles.
 * Dynamic permissions stored in database.
 */
import { useState, useCallback } from 'react'
import { Users, ArrowLeft, Info } from 'lucide-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../api/client'
import type { UserRole, FeaturePermissions } from '../types/api'
import { Card, CardHeader, CardTitle, CardContent } from './Card'
import { useToast } from './Toast'
import { FeatureRow } from './FeatureRow'
import type { Action } from './PermissionCell'
import { SkeletonTable } from './Skeleton'
import { ApiErrorState } from './ApiErrorState'
import { PageHeaderLink } from './PageHeaderLink'
import { PageShell } from './PageShell'
import { PageHeading } from './PageHeading'
import { InfoBanner } from './InfoBanner'
import { RoleLegendChip } from './RoleLegendChip'
import { Badge } from './Badge'
import { Wrapper } from './Wrapper'

type RoleTone = 'purple' | 'blue' | 'slate'

const roleConfig: Record<UserRole, { label: string; tone: RoleTone }> = {
  admin: { label: 'Admin', tone: 'purple' },
  editor: { label: 'Editor', tone: 'blue' },
  viewer: { label: 'Viewer', tone: 'slate' },
}

export function AdminPermissionsPage() {
  const queryClient = useQueryClient()
  const { addToast } = useToast()
  const [updatingCells, setUpdatingCells] = useState<Set<string>>(new Set())

  const { data, isLoading, error, refetch } = useQuery({
    queryKey: ['adminPermissions'],
    queryFn: () => api.getPermissionsMatrix(),
    staleTime: 60 * 1000,
  })

  const updateMutation = useMutation({
    mutationFn: ({
      role,
      feature,
      canView,
      canEdit,
      canDelete,
    }: {
      role: UserRole
      feature: string
      canView: boolean
      canEdit: boolean
      canDelete: boolean
    }) => api.updatePermission(role, feature, canView, canEdit, canDelete),
    onMutate: ({ role, feature }) => {
      const cellKey = `${role}:${feature}`
      setUpdatingCells((prev) => new Set(prev).add(cellKey))
    },
    onSuccess: (result) => {
      queryClient.invalidateQueries({ queryKey: ['adminPermissions'] })
      addToast({
        type: 'success',
        title: `Permission updated`,
        message: `${result.role} / ${result.feature}`,
        duration: 2000,
      })
    },
    onError: (error) => {
      addToast({
        type: 'error',
        title: 'Failed to update permission',
        message: error instanceof Error ? error.message : 'Unknown error',
      })
    },
    onSettled: (_, __, { role, feature }) => {
      const cellKey = `${role}:${feature}`
      setUpdatingCells((prev) => {
        const next = new Set(prev)
        next.delete(cellKey)
        return next
      })
    },
  })

  const handleUpdate = useCallback(
    (role: UserRole, feature: string, action: Action, newValue: boolean) => {
      if (!data) return

      const currentPerms = data.permissions[role]?.[feature] || {
        view: false,
        edit: false,
        delete: false,
      }

      const updatedPerms = {
        ...currentPerms,
        [action]: newValue,
      }

      if (action !== 'view' && newValue && !updatedPerms.view) {
        updatedPerms.view = true
      }

      if (action === 'view' && !newValue) {
        updatedPerms.edit = false
        updatedPerms.delete = false
      }

      updateMutation.mutate({
        role,
        feature,
        canView: updatedPerms.view,
        canEdit: updatedPerms.edit,
        canDelete: updatedPerms.delete,
      })
    },
    [data, updateMutation]
  )

  const features = data?.features ?? []
  const roles = (data?.roles ?? []).map((r) => r.key as UserRole)
  const permissions = data?.permissions as Record<UserRole, Record<string, FeaturePermissions>> | undefined

  const featurePermissions: Record<string, Record<UserRole, FeaturePermissions>> = {}
  for (const feature of features) {
    featurePermissions[feature.key] = {} as Record<UserRole, FeaturePermissions>
    for (const role of roles) {
      featurePermissions[feature.key][role] = permissions?.[role]?.[feature.key] || {
        view: false,
        edit: false,
        delete: false,
      }
    }
  }

  return (
    <PageShell variant="admin">
      <PageHeading
        title="Permissions Matrix"
        subtitle="Configure feature access for each role"
        actions={
          <>
            <PageHeaderLink href="/admin/users" icon={<Users className="w-4 h-4" />}>
              Users
            </PageHeaderLink>
            <PageHeaderLink href="/" icon={<ArrowLeft className="w-4 h-4" />}>
              Dashboard
            </PageHeaderLink>
          </>
        }
      />

      <Wrapper dir="row" gap="lg" marginBottom="lg">
        {roles.map((role) => {
          const config = roleConfig[role]
          const roleInfo = data?.roles.find((r) => r.key === role)
          return (
            <RoleLegendChip
              key={role}
              tone={config.tone}
              label={config.label}
              description={roleInfo?.description}
            />
          )
        })}
      </Wrapper>

      <Card>
        <CardHeader>
          <CardTitle>Feature Permissions</CardTitle>
        </CardHeader>
        <CardContent padding="none">
          {isLoading ? (
            <SkeletonTable />
          ) : error ? (
            <ApiErrorState
              error={error as Error}
              onRetry={refetch}
              title="Failed to load permissions"
            />
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-slate-200 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    <th className="py-3 px-4 w-64">Feature</th>
                    {roles.map((role) => (
                      <th key={role} className="py-3 px-4">
                        <Badge tone={roleConfig[role].tone} shape="square">
                          {roleConfig[role].label}
                        </Badge>
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {features.map((feature) => (
                    <FeatureRow
                      key={feature.key}
                      featureKey={feature.key}
                      featureName={feature.name}
                      featureDescription={feature.description}
                      permissions={featurePermissions[feature.key]}
                      roles={roles}
                      onUpdate={handleUpdate}
                      updatingCells={updatingCells}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </CardContent>
      </Card>

      <Wrapper marginTop="lg">
        <InfoBanner icon={<Info className="w-5 h-5" />} title="How permissions work">
          <ul className="space-y-1">
            <li><strong>View</strong> - User can see this feature in the dashboard</li>
            <li><strong>Edit</strong> - User can create and modify data (requires View)</li>
            <li><strong>Delete</strong> - User can remove data (requires View)</li>
          </ul>
        </InfoBanner>
      </Wrapper>
    </PageShell>
  )
}
