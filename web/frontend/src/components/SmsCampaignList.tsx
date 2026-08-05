import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Card, CardContent, CardHeader, CardTitle } from './Card'
import { Button } from './Button'
import { Badge } from './Badge'
import { EmptyState } from './EmptyState'
import { SkeletonTable } from './Skeleton'
import { useSmsCampaigns, useMarkSmsCampaignSent } from '../hooks/useApi'
import { useToast } from './Toast'
import { SmsSendDialog } from './SmsSendDialog'
import { formatNumber } from '../utils/formatters'
import type { SmsCampaignSummary } from '../types/api'

// ─── SmsCampaignList ─────────────────────────────────────────────────────────
//
// Frozen rosters, and the one action that has to happen outside this app:
// marking when the file actually went to the SMS provider. Results are
// measured from that date, so a campaign sitting here unsent is a campaign
// that cannot be read yet — the row says so rather than showing empty numbers.

function formatDateTime(iso: string | null): string {
  if (!iso) return '—'
  const d = new Date(iso)
  return Number.isNaN(d.getTime())
    ? '—'
    : d.toLocaleString(undefined, {
        year: 'numeric', month: 'short', day: 'numeric',
        hour: '2-digit', minute: '2-digit',
      })
}

export const SmsCampaignList = memo(function SmsCampaignList() {
  const { t } = useTranslation()
  const { data, isLoading } = useSmsCampaigns()
  const markSent = useMarkSmsCampaignSent()
  const { addToast } = useToast()
  const [sending, setSending] = useState<SmsCampaignSummary | null>(null)

  const campaigns = data?.campaigns ?? []

  function handleMarkSent(campaign: string) {
    markSent.mutate(
      { campaign },
      {
        onSuccess: () =>
          addToast({ type: 'success', title: t('sms.markedSent', { campaign }) }),
        onError: () =>
          addToast({ type: 'error', title: t('sms.markSentFailed') }),
      },
    )
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center gap-2">
          <Badge tone="slate" shape="tag">{t('sms.step', { n: 2 })}</Badge>
          <CardTitle>{t('sms.campaignsTitle')}</CardTitle>
        </div>
        <p className="text-xs text-slate-500 mt-0.5">{t('sms.campaignsDesc')}</p>
      </CardHeader>

      <CardContent>
        {isLoading ? (
          <SkeletonTable />
        ) : campaigns.length === 0 ? (
          <EmptyState
            message={t('sms.noCampaigns')}
            hint={t('sms.noCampaignsHint')}
          />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-[11px] uppercase tracking-wide text-slate-500 border-b border-slate-200">
                  <th className="py-2 pr-3 font-medium">{t('sms.campaign')}</th>
                  <th className="py-2 px-3 font-medium text-right">{t('sms.toSend')}</th>
                  <th className="py-2 px-3 font-medium text-right">{t('sms.control')}</th>
                  <th className="py-2 px-3 font-medium">{t('sms.exported')}</th>
                  <th className="py-2 px-3 font-medium">{t('sms.sent')}</th>
                  <th className="py-2 pl-3" />
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-100">
                {campaigns.map((c) => (
                  <tr key={c.campaign}>
                    <td className="py-2.5 pr-3">
                      <div className="font-medium text-slate-800">{c.campaign}</div>
                      <div className="text-[11px] text-slate-500">
                        {t(`sms.basis${c.ltvBasis === 'margin' ? 'Margin' : 'Revenue'}`)}
                        {c.promocode ? ` · ${c.promocode}` : ''}
                      </div>
                    </td>
                    <td className="py-2.5 px-3 text-right tabular-nums text-slate-700">
                      {formatNumber(c.target)}
                    </td>
                    <td className="py-2.5 px-3 text-right tabular-nums text-slate-700">
                      {formatNumber(c.holdout)}
                    </td>
                    <td className="py-2.5 px-3 text-slate-600 text-xs whitespace-nowrap">
                      {formatDateTime(c.exportedAt)}
                    </td>
                    <td className="py-2.5 px-3 text-xs whitespace-nowrap">
                      {c.sentAt ? (
                        <span className="text-slate-600">{formatDateTime(c.sentAt)}</span>
                      ) : (
                        <Badge tone="orange">{t('sms.notSent')}</Badge>
                      )}
                    </td>
                    <td className="py-2.5 pl-3 text-right">
                      {!c.sentAt && (
                        <div className="flex justify-end gap-2">
                          <Button size="sm" onClick={() => setSending(c)}>
                            {t('sms.send')}
                          </Button>
                          {/* Kept for the manual path: a file handed to the
                              provider outside this app still needs a send date,
                              or its results cannot be measured. */}
                          <Button
                            size="sm"
                            variant="secondary"
                            onClick={() => handleMarkSent(c.campaign)}
                            disabled={markSent.isPending}
                          >
                            {t('sms.markSent')}
                          </Button>
                        </div>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </CardContent>

      {sending && (
        <SmsSendDialog campaign={sending} onClose={() => setSending(null)} />
      )}
    </Card>
  )
})
