import { Fragment, memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Card, CardContent, CardHeader, CardTitle } from './Card'
import { Button } from './Button'
import { Badge } from './Badge'
import { EmptyState } from './EmptyState'
import { SkeletonTable } from './Skeleton'
import { useSmsCampaigns, useMarkSmsCampaignSent } from '../hooks/useApi'
import { useToast } from './Toast'
import { SmsSendDialog } from './SmsSendDialog'
import { BarChart3, ChevronDown, ChevronRight } from 'lucide-react'
import { formatCurrency, formatNumber } from '../utils/formatters'
import { describeFrozenCriteria } from '../utils/smsAudience'
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

/** `onSelect` wires the list to the results block below it. Optional, so the
 *  list still stands alone — and still tests alone. */
interface SmsCampaignListProps {
  selected?: string | null
  onSelect?: (campaign: string) => void
}

export const SmsCampaignList = memo(function SmsCampaignList({
  selected, onSelect,
}: SmsCampaignListProps = {}) {
  const { t } = useTranslation()
  const { data, isLoading } = useSmsCampaigns()
  const markSent = useMarkSmsCampaignSent()
  const { addToast } = useToast()
  const [sending, setSending] = useState<SmsCampaignSummary | null>(null)
  const [open, setOpen] = useState<string | null>(null)

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
                  <Fragment key={c.campaign}>
                  <tr className={selected === c.campaign ? 'bg-purple-50/60' : undefined}>
                    <td className="py-2.5 pr-3">
                      {/* The name opens the campaign. Everything a campaign was
                          — the audience, the text, the bill — was recorded from
                          the start and readable nowhere. */}
                      <button
                        type="button"
                        onClick={() => setOpen(open === c.campaign ? null : c.campaign)}
                        aria-expanded={open === c.campaign}
                        className="flex items-center gap-1 font-medium text-slate-800
                                   hover:text-purple-800"
                      >
                        {open === c.campaign
                          ? <ChevronDown className="w-3.5 h-3.5" />
                          : <ChevronRight className="w-3.5 h-3.5" />}
                        {c.campaign}
                      </button>
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
                      {/* A sent campaign's only remaining question is what it
                          did. The results block sat directly below with its own
                          campaign picker and no connection to this table, so
                          reading last month's campaign meant noticing a second
                          dropdown existed. */}
                      {c.sentAt && onSelect && (
                        <Button
                          size="sm"
                          variant={selected === c.campaign ? 'primary' : 'secondary'}
                          onClick={() => onSelect(c.campaign)}
                        >
                          <BarChart3 className="w-3.5 h-3.5" />
                          {t('sms.viewResults')}
                        </Button>
                      )}
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

                  {open === c.campaign && (
                    <tr>
                      <td colSpan={6} className="bg-slate-50/70 px-3 py-3">
                        <div className="grid gap-4 sm:grid-cols-2">
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-slate-500">
                              {t('sms.detailsMessage')}
                            </p>
                            {c.messageText ? (
                              <>
                                <pre className="mt-1 whitespace-pre-wrap text-sm
                                                text-slate-800 font-sans">
                                  {c.messageText}
                                </pre>
                                <p className="mt-1 text-xs text-slate-500 tabular-nums">
                                  {t('sms.detailsCost', {
                                    recipients: formatNumber(c.recipientsSent ?? c.target),
                                    parts: c.messageParts ?? 1,
                                    price: c.pricePerPart ?? 0,
                                    total: c.costTotal == null
                                      ? '—' : formatCurrency(c.costTotal),
                                  })}
                                </p>
                              </>
                            ) : (
                              <p className="mt-1 text-sm text-slate-500">
                                {t('sms.detailsNoMessage')}
                              </p>
                            )}
                            {c.notes && (
                              <p className="mt-1.5 text-[11px] text-amber-700 leading-snug">
                                {c.notes}
                              </p>
                            )}
                            {(c.delivered != null && c.delivered > 0) && (
                              <p className="mt-2 text-xs text-slate-600 tabular-nums">
                                {t('sms.detailsDelivery', {
                                  delivered: formatNumber(c.delivered),
                                  undelivered: formatNumber(c.undelivered ?? 0),
                                })}
                              </p>
                            )}
                          </div>
                          <div>
                            <p className="text-[11px] uppercase tracking-wide text-slate-500">
                              {t('sms.detailsAudience')}
                            </p>
                            <ul className="mt-1 space-y-0.5 text-sm text-slate-700">
                              {describeFrozenCriteria(c.criteria, t).map((line) => (
                                <li key={line}>{line}</li>
                              ))}
                            </ul>
                          </div>
                        </div>
                      </td>
                    </tr>
                  )}
                  </Fragment>
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
