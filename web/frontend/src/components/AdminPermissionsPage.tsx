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

const roleConfig: Record<UserRole, { label: string; color: string }> = {
  admin: { label: 'Admin', color: 'bg-purple-100 text-purple-700 border-purple-200' },
  editor: { label: 'Editor', color: 'bg-blue-100 text-blue-700 border-blue-200' },
  viewer: { label: 'Viewer', color: 'bg-slate-100 text-slate-600 border-slate-200' },
}

export function AdminPermissionsPage() {
  const queryClient = useQueryClient()
  const { addToast } = useToast()
  const [updatingCells, setUpdatingCells] = useState<Set<string>>(new Set())

  const { data, isLoading, error } = useQuery({
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
    <main className="p-3 sm:p-6 lg:p-8 max-w-[1400px] mx-auto">
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Permissions Matrix</h1>
          <p className="text-sm text-slate-500 mt-1">
            Configure feature access for each role
          </p>
        </div>
        <div className="flex gap-3">
          <a
            href="/admin/users"
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-slate-600 hover:text-slate-900 hover:bg-slate-100 rounded-lg transition-colors"
          >
            <Users className="w-4 h-4" />
            Users
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

      <div className="flex gap-4 mb-6">
        {roles.map((role) => {
          const config = roleConfig[role]
          const roleInfo = data?.roles.find((r) => r.key === role)
          return (
            <div
              key={role}
              className={`px-3 py-1.5 rounded-lg border ${config.color}`}
            >
              <span className="font-medium">{config.label}</span>
              {roleInfo && (
                <span className="ml-2 text-xs opacity-75">{roleInfo.description}</span>
              )}
            </div>
          )
        })}
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Feature Permissions</CardTitle>
        </CardHeader>
        <CardContent padding="none">
          {isLoading ? (
            <div className="py-12 text-center text-slate-500">
              Loading permissions...
            </div>
          ) : error ? (
            <div className="py-12 text-center text-red-500">
              Error loading permissions. Please try again.
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead>
                  <tr className="border-b border-slate-200 text-left text-xs font-medium text-slate-500 uppercase tracking-wider">
                    <th className="py-3 px-4 w-64">Feature</th>
                    {roles.map((role) => (
                      <th key={role} className="py-3 px-4">
                        <span className={`px-2 py-0.5 rounded text-xs font-medium ${roleConfig[role].color}`}>
                          {roleConfig[role].label}
                        </span>
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

      <div className="mt-6 p-4 bg-blue-50 border border-blue-100 rounded-lg">
        <div className="flex gap-3">
          <Info className="w-5 h-5 text-blue-500 flex-shrink-0 mt-0.5" />
          <div className="text-sm text-blue-800">
            <p className="font-medium">How permissions work</p>
            <ul className="mt-1 space-y-1 text-blue-700">
              <li><strong>View</strong> - User can see this feature in the dashboard</li>
              <li><strong>Edit</strong> - User can create and modify data (requires View)</li>
              <li><strong>Delete</strong> - User can remove data (requires View)</li>
            </ul>
          </div>
        </div>
      </div>
    </main>
  )
}
