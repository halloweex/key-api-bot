import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Card, CardContent, CardHeader, CardTitle } from './Card'
import { Select } from './Select'
import { Badge } from './Badge'
import { Checkbox } from './Checkbox'
import { EmptyState } from './EmptyState'
import { SkeletonCard } from './Skeleton'
import { SmsLiftInterval } from './SmsLiftInterval'
import { SmsResultsGuide } from './SmsResultsGuide'
import { useSmsCampaigns, useSmsCampaignResults } from '../hooks/useApi'
import { formatCurrency, formatNumber } from '../utils/formatters'
import type { SmsComparison, SmsGroupStats, SmsTier } from '../types/api'
import { ApiError } from '../api/client'

// ─── SmsCampaignResults ──────────────────────────────────────────────────────
//
// Reads a campaign against its control.
//
// The layout deliberately puts the control group beside the messaged one at
// every level. The target's own conversion is the number people want to quote,
// and on its own it is meaningless — Tier 1 buys at ~46% without any message.
// Showing the pair side by side makes the comparison the obvious reading.

const WINDOWS = [7, 14, 30, 60, 90]

function GroupColumn({ label, stats, muted }: {
  label: string
  stats: SmsGroupStats
  muted?: boolean
}) {
  const { t } = useTranslation()
  const rate = stats.contacts ? (100 * stats.converted) / stats.contacts : 0

  return (
    <div className={muted ? 'opacity-80' : undefined}>
      <div className="text-[11px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className="text-xl font-semibold text-slate-800 tabular-nums">
        {rate.toFixed(1)}%
      </div>
      <div className="text-[11px] text-slate-500 tabular-nums">
        {formatNumber(stats.converted)} / {formatNumber(stats.contacts)} {t('sms.bought')}
      </div>
    </div>
  )
}

function Verdict({ comparison }: { comparison: SmsComparison }) {
  const { t } = useTranslation()

  return comparison.significant ? (
    <Badge tone="green">
      {t('sms.verdictProven', { lift: comparison.liftPp.toFixed(1) })}
    </Badge>
  ) : (
    <Badge tone="slate">{t('sms.verdictInconclusive')}</Badge>
  )
}

function ComparisonBlock({ comparison }: { comparison: SmsComparison }) {
  const { t } = useTranslation()

  return (
    <div className="space-y-2">
      <SmsLiftInterval comparison={comparison} />
      <dl className="grid grid-cols-2 gap-x-3 gap-y-1 text-[11px]">
        <dt className="text-slate-500">{t('sms.lift')}</dt>
        <dd className="text-right text-slate-800 font-medium tabular-nums">
          {comparison.liftPp > 0 ? '+' : ''}{comparison.liftPp.toFixed(1)} {t('sms.pp')}
        </dd>
        <dt className="text-slate-500">{t('sms.pValue')}</dt>
        <dd className="text-right text-slate-800 tabular-nums">
          {comparison.pValue < 0.001 ? '<0.001' : comparison.pValue.toFixed(3)}
        </dd>
        {/* Revenue beside margin: revenue is the figure people recognise,
            margin is the one that decides whether the campaign paid. */}
        <dt className="text-slate-500">{t('sms.incrementalRevenuePerContact')}</dt>
        <dd className="text-right text-slate-800 tabular-nums">
          {formatCurrency(comparison.incrementalRevenuePerContact)}
        </dd>
        <dt className="text-slate-500">{t('sms.incrementalRevenueTotal')}</dt>
        <dd className="text-right text-slate-800 font-medium tabular-nums">
          {formatCurrency(comparison.incrementalRevenueTotal)}
        </dd>
        <dt className="text-slate-500">{t('sms.incrementalPerContact')}</dt>
        <dd className="text-right text-slate-800 tabular-nums">
          {formatCurrency(comparison.incrementalMarginPerContact)}
        </dd>
        <dt className="text-slate-500">{t('sms.incrementalTotal')}</dt>
        <dd className="text-right text-slate-800 font-medium tabular-nums">
          {formatCurrency(comparison.incrementalMarginTotal)}
        </dd>
      </dl>
    </div>
  )
}

export const SmsCampaignResults = memo(function SmsCampaignResults() {
  const { t } = useTranslation()
  const [campaign, setCampaign] = useState<string | null>(null)
  const [windowDays, setWindowDays] = useState(30)
  const [deliveredOnly, setDeliveredOnly] = useState(false)

  const { data: list } = useSmsCampaigns()
  const sent = useMemo(
    () => (list?.campaigns ?? []).filter((c) => c.sentAt),
    [list],
  )

  const selected = campaign ?? sent[0]?.campaign ?? null
  const { data, isLoading, error } = useSmsCampaignResults(
    selected, windowDays, deliveredOnly,
  )
  // Anyone the gateway never took is in the target arm without ever having
  // been messaged, which pulls the measured lift toward nothing. Saying so is
  // more use than quietly dropping them.
  const notSent = data?.overall.target.notSent ?? 0

  if (sent.length === 0) {
    return (
      <Card>
        <CardHeader>
          <CardTitle>{t('sms.resultsTitle')}</CardTitle>
        </CardHeader>
        <CardContent>
          <EmptyState
            message={t('sms.noSentCampaigns')}
            hint={t('sms.noSentCampaignsHint')}
          />
        </CardContent>
      </Card>
    )
  }

  const tiers: SmsTier[] = (data?.segments ?? []).map((s) => s.tier)

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <div className="flex items-center gap-2">
              <Badge tone="slate" shape="tag">{t('sms.step', { n: 3 })}</Badge>
              <CardTitle>{t('sms.resultsTitle')}</CardTitle>
            </div>
            <p className="text-xs text-slate-500 mt-0.5">{t('sms.resultsDesc')}</p>
          </div>
          <div className="flex items-center gap-2">
            <Select
              options={sent.map((c) => ({ value: c.campaign, label: c.campaign }))}
              value={selected}
              onChange={setCampaign}
              allowEmpty={false}
              variant="compact"
              aria-label={t('sms.campaignName')}
            />
            <Select
              options={WINDOWS.map((w) => ({
                value: String(w),
                label: t('sms.windowDays', { days: w }),
              }))}
              value={String(windowDays)}
              onChange={(v) => setWindowDays(Number(v) || 30)}
              allowEmpty={false}
              variant="compact"
              aria-label={t('sms.window')}
            />
          </div>
        </div>
      </CardHeader>

      <CardContent>
        {isLoading && <SkeletonCard />}

        {error && (
          <EmptyState
            message={
              error instanceof ApiError && error.status === 409
                ? t('sms.notSentYet')
                : t('sms.resultsFailed')
            }
          />
        )}

        {data && !isLoading && (
          <>
            <SmsResultsGuide />

            {/* ── How much of the arm was actually treated ────────────── */}
            {notSent > 0 && (
              <div className="mb-4 text-xs text-amber-800 bg-amber-50 rounded-md px-3 py-2 leading-snug">
                {t('sms.notSentWarning', { n: formatNumber(notSent) })}
              </div>
            )}

            <label className="mb-4 flex items-start gap-2 cursor-pointer">
              <Checkbox checked={deliveredOnly} onChange={setDeliveredOnly} size="sm" />
              <span className="text-xs text-slate-600 leading-snug">
                <span className="font-medium text-slate-700">
                  {t('sms.deliveredOnlyLabel')}
                </span>
                {' — '}
                {t('sms.deliveredOnlyHint')}
              </span>
            </label>

            {/* ── Overall ─────────────────────────────────────────────── */}
            {data.overall.comparison && (
              <div className="rounded-lg border border-slate-200 p-3 sm:p-4 mb-4">
                <div className="flex flex-wrap items-center justify-between gap-2 mb-3">
                  <span className="text-sm font-medium text-slate-700">
                    {t('sms.overall')}
                  </span>
                  <Verdict comparison={data.overall.comparison} />
                </div>
                <div className="grid gap-4 sm:grid-cols-2">
                  <div className="flex gap-6">
                    <GroupColumn label={t('sms.messaged')} stats={data.overall.target} />
                    <GroupColumn label={t('sms.control')} stats={data.overall.holdout} muted />
                  </div>
                  <ComparisonBlock comparison={data.overall.comparison} />
                </div>
              </div>
            )}

            {/* ── Per tier ────────────────────────────────────────────── */}
            <div className="grid gap-3 sm:grid-cols-3">
              {data.segments.map((s) => (
                <div key={s.tier} className="rounded-lg border border-slate-200 p-3">
                  <div className="flex items-center justify-between gap-2 mb-2">
                    <span className="text-xs font-medium text-slate-700">
                      {t(`sms.tier.${s.tier}`)}
                    </span>
                    {s.comparison && <Verdict comparison={s.comparison} />}
                  </div>
                  <div className="flex gap-4 mb-3">
                    <GroupColumn label={t('sms.messaged')} stats={s.target} />
                    <GroupColumn label={t('sms.control')} stats={s.holdout} muted />
                  </div>
                  {s.comparison ? (
                    <ComparisonBlock comparison={s.comparison} />
                  ) : (
                    <p className="text-[11px] text-slate-500">{t('sms.noControlInTier')}</p>
                  )}
                </div>
              ))}
            </div>

            {data.promocode && (
              <p className="mt-3 text-xs text-slate-500">
                {t('sms.promoOrders', {
                  code: data.promocode,
                  count: data.overall.target.promoOrders,
                })}
              </p>
            )}

            <p className="mt-3 text-[11px] text-slate-400 leading-snug">
              {t('sms.deliveryCaveat')}
            </p>

            {tiers.length === 0 && <EmptyState message={t('sms.noRosterData')} />}
          </>
        )}
      </CardContent>
    </Card>
  )
})
