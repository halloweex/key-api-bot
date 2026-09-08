import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { ChevronDown, ChevronRight } from 'lucide-react'
import type {
  AccessPreset, AdminUser, TabFeature, UserRole, UserStatus,
} from '../types/api'
import { BadgeSelect } from './BadgeSelect'
import { Tr, Td } from './DataTable'
import { UserAccessEditor } from './UserAccessEditor'
import { tabSummary } from '../utils/access'
import { UserAvatar } from './UserAvatar'

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

// Levels, not areas. `marketer` was here and was the mistake: an area wearing
// a level's clothes, which made "a marketer who may only look" unsayable. It
// is a tab preset now, so the pair (level, tabs) says both.
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
  onFeaturesChange: (userId: number, features: TabFeature[] | null) => void
  onPreset: (userId: number, preset: string) => void
  /** Open the tab editor under this row. */
  expanded: boolean
  onToggleExpanded: (userId: number) => void
  tabs?: readonly TabFeature[]
  /** What this account's role opens — what "as the role" means for them. */
  roleFeatures?: readonly TabFeature[]
  presets?: AccessPreset[]
  isUpdating: boolean
}

export const UserRow = memo(function UserRow({
  user,
  onRoleChange,
  onStatusChange,
  onFeaturesChange,
  onPreset,
  expanded,
  onToggleExpanded,
  tabs,
  roleFeatures,
  presets,
  isUpdating,
}: UserRowProps) {
  const { t } = useTranslation()
  const displayName = user.first_name
    ? `${user.first_name}${user.last_name ? ` ${user.last_name}` : ''}`
    : user.username
      ? `@${user.username}`
      : `User ${user.user_id}`

  const lastActivity = user.last_activity
    ? new Date(user.last_activity).toLocaleDateString()
    : 'Never'

  const summary = tabSummary(
    user.allowed_features,
    (tab) => t(`access.tab.${tab}`),
    t('access.asRole'),
    t('access.noTabs'),
    roleFeatures,
  )

  return (
    <>
    <Tr variant="admin" faded={isUpdating}>
      <Td variant="admin">
        <div className="flex items-center gap-3">
          {/* UserAvatar has onError → falls back to a generated boring-avatar
              when Telegram's userpic URL 404s (avatars change when the user
              updates their profile photo). */}
          <UserAvatar name={displayName} photoUrl={user.photo_url} size={36} />
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
        {/* The summary is the control: reading "Traffic" beside somebody's
            name is the answer to "what can they see", and the chevron is how
            you change it. A second dropdown here would not fit a set. */}
        <button
          type="button"
          onClick={() => onToggleExpanded(user.user_id)}
          disabled={isUpdating}
          aria-expanded={expanded}
          className="flex items-center gap-1.5 text-sm text-slate-600 hover:text-slate-900 disabled:opacity-50"
        >
          {expanded
            ? <ChevronDown className="w-4 h-4 shrink-0" aria-hidden />
            : <ChevronRight className="w-4 h-4 shrink-0" aria-hidden />}
          <span className="text-left">{summary}</span>
        </button>
      </Td>

      <Td variant="admin">
        <span className="text-sm text-slate-600">{lastActivity}</span>
      </Td>
    </Tr>

    {expanded && (
      <Tr variant="admin" hover={false} faded={isUpdating}>
        <Td variant="admin" colSpan={5}>
          <UserAccessEditor
            value={user.allowed_features}
            tabs={tabs}
            roleFeatures={roleFeatures}
            roleName={t(`profile.${user.role}`, user.role)}
            presets={presets}
            disabled={isUpdating}
            onChange={(features) => onFeaturesChange(user.user_id, features)}
            onPreset={(preset) => onPreset(user.user_id, preset)}
          />
        </Td>
      </Tr>
    )}
    </>
  )
})
