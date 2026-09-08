import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { UserPlus } from 'lucide-react'
import { api } from '../api/client'
import type { AccessPreset, AccessRequest, TabFeature } from '../types/api'
import { Button } from './Button'
import { Card, CardHeader, CardTitle, CardContent } from './Card'
import { UserAccessEditor } from './UserAccessEditor'
import { UserAvatar } from './UserAvatar'
import { Wrapper } from './Wrapper'

// ─── AccessRequestsCard ──────────────────────────────────────────────────────
//
// Who is waiting to be let in, and the tabs they are let in *with*.
//
// A request lands in the bot's list, because Telegram is the only door; the
// dashboard's list gains a row when somebody approves. Until this card
// existed the admin page showed the second list only, so an incoming request
// was invisible here and the decision could be taken from a phone or not at
// all. Approving happens with the tab set already chosen, rather than granting
// first and narrowing after — the same order the bot's keyboard now uses.
//
// Renders nothing when the queue is empty: an admin page with a permanent
// empty box teaches people to stop looking at it.

const DEFAULT_PRESET = 'standard'

function displayName(request: AccessRequest): string {
  if (request.first_name) {
    return `${request.first_name}${request.last_name ? ` ${request.last_name}` : ''}`
  }
  if (request.username) return `@${request.username}`
  return `User ${request.user_id}`
}

const AccessRequestRow = memo(function AccessRequestRow({
  request,
  presets,
  tabs,
  busy,
  onApprove,
  onDeny,
}: {
  request: AccessRequest
  presets?: AccessPreset[]
  tabs?: readonly TabFeature[]
  busy: boolean
  onApprove: (userId: number, features: TabFeature[]) => void
  onDeny: (userId: number) => void
}) {
  const { t } = useTranslation()
  const fallback = presets?.find((p) => p.key === DEFAULT_PRESET)?.features ?? []
  const [features, setFeatures] = useState<TabFeature[]>(fallback)

  const name = displayName(request)
  const asked = request.requested_at
    ? new Date(request.requested_at).toLocaleString()
    : ''

  return (
    <Wrapper dir="column" gap="sm" paddingY="sm">
      <Wrapper dir="row-responsive" gap="md" align="center" justify="between">
        <Wrapper dir="row" gap="md" align="center">
          <UserAvatar name={name} size={36} />
          <div>
            <p className="font-medium text-slate-900">{name}</p>
            <p className="text-xs text-slate-500">
              ID: {request.user_id}
              {asked ? ` · ${asked}` : ''}
              {request.denial_count > 0
                ? ` · ${t('access.deniedBefore', { count: request.denial_count })}`
                : ''}
            </p>
          </div>
        </Wrapper>
        <Wrapper dir="row" gap="sm" align="center">
          <Button
            variant="secondary"
            size="sm"
            disabled={busy}
            onClick={() => onDeny(request.user_id)}
          >
            {t('access.deny')}
          </Button>
          <Button
            variant="primary"
            size="sm"
            disabled={busy}
            onClick={() => onApprove(request.user_id, features)}
          >
            {t('access.approve')}
          </Button>
        </Wrapper>
      </Wrapper>

      {/* The tabs are chosen before the grant, not after it. */}
      <UserAccessEditor
        value={features}
        tabs={tabs}
        presets={presets}
        disabled={busy}
        onChange={(next) => setFeatures(next ?? [])}
        onPreset={(key) => {
          const preset = presets?.find((p) => p.key === key)
          if (preset) setFeatures(preset.features)
        }}
      />
    </Wrapper>
  )
})

export const AccessRequestsCard = memo(function AccessRequestsCard({
  presets,
  tabs,
}: {
  presets?: AccessPreset[]
  tabs?: readonly TabFeature[]
}) {
  const { t } = useTranslation()
  const queryClient = useQueryClient()
  const [busy, setBusy] = useState<number | null>(null)

  const { data } = useQuery({
    queryKey: ['accessRequests'],
    queryFn: () => api.getAccessRequests(),
    // Somebody may be waiting on the other end of Telegram, so this is the one
    // admin query worth refetching on its own.
    refetchInterval: 60 * 1000,
    staleTime: 30 * 1000,
    retry: false,
  })

  const settle = {
    onMutate: ({ userId }: { userId: number }) => setBusy(userId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['accessRequests'] })
      queryClient.invalidateQueries({ queryKey: ['adminUsers'] })
    },
    onSettled: () => setBusy(null),
  }

  const approve = useMutation({
    mutationFn: ({ userId, features }: { userId: number; features: TabFeature[] }) =>
      api.approveAccessRequest(userId, features),
    ...settle,
  })

  const deny = useMutation({
    mutationFn: ({ userId }: { userId: number }) => api.denyAccessRequest(userId),
    ...settle,
  })

  const requests = data?.requests ?? []
  if (requests.length === 0) return null

  return (
    <Card>
      <CardHeader>
        <Wrapper dir="row" gap="sm" align="center">
          <UserPlus className="w-4 h-4 text-slate-500" aria-hidden />
          <CardTitle>
            {t('access.requestsTitle')} ({requests.length})
          </CardTitle>
        </Wrapper>
      </CardHeader>
      <CardContent>
        <div className="divide-y divide-slate-100">
          {requests.map((request) => (
            <AccessRequestRow
              key={request.user_id}
              request={request}
              presets={presets}
              tabs={tabs}
              busy={busy === request.user_id}
              onApprove={(userId, features) => approve.mutate({ userId, features })}
              onDeny={(userId) => deny.mutate({ userId })}
            />
          ))}
        </div>
      </CardContent>
    </Card>
  )
})
