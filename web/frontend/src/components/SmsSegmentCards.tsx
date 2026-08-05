import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Card, CardContent, CardHeader, CardTitle } from './Card'
import { Button } from './Button'
import { Checkbox } from './Checkbox'
import { Select } from './Select'
import { Badge } from './Badge'
import { ApiErrorState } from './ApiErrorState'
import { SkeletonCard } from './Skeleton'
import { useSmsSegments } from '../hooks/useApi'
import { formatCurrency, formatNumber } from '../utils/formatters'
import type { SmsLtvBasis, SmsSegment, SmsTier } from '../types/api'

// ─── SmsSegmentCards ─────────────────────────────────────────────────────────
//
// Pick the criteria, see the three tiers, take the file.
//
// The export is the moment a campaign becomes real, so "freeze" lives right
// next to the download button rather than buried in settings: the roster has
// to be recorded at that instant or the campaign can never be measured.

const TIER_STYLE: Record<SmsTier, { tone: 'purple' | 'blue' | 'orange'; order: number }> = {
  VIP: { tone: 'purple', order: 0 },
  CORE: { tone: 'blue', order: 1 },
  REACTIVATION: { tone: 'orange', order: 2 },
}

const CAMPAIGN_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$/

function TierCard({ segment }: { segment: SmsSegment }) {
  const { t } = useTranslation()
  const style = TIER_STYLE[segment.tier] ?? TIER_STYLE.CORE

  return (
    <Card>
      <CardContent>
        <div className="flex items-start justify-between gap-2">
          <div>
            <Badge tone={style.tone}>{t(`sms.tier.${segment.tier}`)}</Badge>
            <div className="mt-2 text-2xl font-semibold text-slate-800 tabular-nums">
              {formatNumber(segment.total)}
            </div>
            <div className="text-xs text-slate-500">{t('sms.contacts')}</div>
          </div>
          <div className="text-right text-xs text-slate-500 space-y-0.5 tabular-nums">
            <div>
              <span className="text-slate-700 font-medium">{formatNumber(segment.target)}</span>{' '}
              {t('sms.toSend')}
            </div>
            <div>
              <span className="text-slate-700 font-medium">{formatNumber(segment.holdout)}</span>{' '}
              {t('sms.control')}
            </div>
          </div>
        </div>

        <dl className="mt-3 pt-3 border-t border-slate-100 grid grid-cols-3 gap-2 text-xs">
          <div>
            <dt className="text-slate-500">{t('sms.avgLtv')}</dt>
            <dd className="text-slate-800 font-medium tabular-nums">
              {formatCurrency(segment.avgLtv)}
            </dd>
          </div>
          <div>
            <dt className="text-slate-500">{t('sms.avgOrders')}</dt>
            <dd className="text-slate-800 font-medium tabular-nums">{segment.avgOrders}</dd>
          </div>
          <div>
            <dt className="text-slate-500">{t('sms.avgRecency')}</dt>
            <dd className="text-slate-800 font-medium tabular-nums">
              {segment.avgRecencyDays} {t('sms.daysShort')}
            </dd>
          </div>
        </dl>
      </CardContent>
    </Card>
  )
}

export const SmsSegmentCards = memo(function SmsSegmentCards() {
  const { t } = useTranslation()
  const [ltvBasis, setLtvBasis] = useState<SmsLtvBasis>('margin')
  const [campaign, setCampaign] = useState('')
  const [promocode, setPromocode] = useState('')
  const [freeze, setFreeze] = useState(false)

  const params = useMemo(
    () => `ltv_basis=${ltvBasis}&include_customers=false`,
    [ltvBasis],
  )
  const { data, isLoading, error, refetch } = useSmsSegments(params)

  const campaignValid = CAMPAIGN_PATTERN.test(campaign)
  const canExport = !freeze || campaignValid

  const segments = useMemo(
    () =>
      [...(data?.segments ?? [])].sort(
        (a, b) => (TIER_STYLE[a.tier]?.order ?? 9) - (TIER_STYLE[b.tier]?.order ?? 9),
      ),
    [data],
  )

  function handleExport() {
    const p = new URLSearchParams({ ltv_basis: ltvBasis })
    if (campaignValid) p.set('campaign', campaign)
    if (freeze) p.set('freeze', 'true')
    if (promocode.trim()) p.set('promocode', promocode.trim())
    window.open(`/api/customers/sms-segments/export/csv?${p.toString()}`, '_blank')
  }

  if (error) {
    return <ApiErrorState error={error} onRetry={() => refetch()} />
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <CardTitle>{t('sms.segmentsTitle')}</CardTitle>
            <p className="text-xs text-slate-500 mt-0.5">{t('sms.segmentsDesc')}</p>
          </div>
          <Select
            options={[
              { value: 'margin', label: t('sms.basisMargin') },
              { value: 'revenue', label: t('sms.basisRevenue') },
            ]}
            value={ltvBasis}
            onChange={(v) => setLtvBasis((v as SmsLtvBasis) || 'margin')}
            allowEmpty={false}
            variant="compact"
            aria-label={t('sms.basisLabel')}
          />
        </div>
      </CardHeader>

      <CardContent>
        {isLoading ? (
          <div className="grid gap-3 sm:grid-cols-3">
            <SkeletonCard />
            <SkeletonCard />
            <SkeletonCard />
          </div>
        ) : (
          <>
            <div className="grid gap-3 sm:grid-cols-3">
              {segments.map((s) => (
                <TierCard key={s.tier} segment={s} />
              ))}
            </div>

            <div className="mt-3 text-xs text-slate-500 tabular-nums">
              {t('sms.totals', {
                total: formatNumber(data?.totals.customers ?? 0),
                target: formatNumber(data?.totals.target ?? 0),
                holdout: formatNumber(data?.totals.holdout ?? 0),
              })}
            </div>

            {/* ── Export ─────────────────────────────────────────────── */}
            <div className="mt-4 pt-4 border-t border-slate-100 space-y-3">
              <div className="flex flex-wrap items-end gap-3">
                <label className="text-xs text-slate-600">
                  <span className="block mb-1">{t('sms.campaignName')}</span>
                  <input
                    type="text"
                    value={campaign}
                    onChange={(e) => setCampaign(e.target.value)}
                    placeholder="aug-promo"
                    className="px-2 py-1.5 text-sm bg-white border border-slate-200 rounded-md
                               text-slate-700 focus:outline-none focus:ring-2
                               focus:ring-purple-500/30 focus:border-purple-400"
                  />
                </label>
                <label className="text-xs text-slate-600">
                  <span className="block mb-1">{t('sms.promocodeOptional')}</span>
                  <input
                    type="text"
                    value={promocode}
                    onChange={(e) => setPromocode(e.target.value)}
                    placeholder="KS-AUG"
                    maxLength={40}
                    className="px-2 py-1.5 text-sm bg-white border border-slate-200 rounded-md
                               text-slate-700 focus:outline-none focus:ring-2
                               focus:ring-purple-500/30 focus:border-purple-400"
                  />
                </label>
                <Button onClick={handleExport} disabled={!canExport} size="sm">
                  {t('sms.downloadCsv')}
                </Button>
              </div>

              <label className="flex items-start gap-2 cursor-pointer">
                <Checkbox checked={freeze} onChange={setFreeze} size="sm" />
                <span className="text-xs text-slate-600 leading-snug">
                  <span className="font-medium text-slate-700">{t('sms.freezeLabel')}</span>
                  {' — '}
                  {t('sms.freezeHint')}
                </span>
              </label>

              {freeze && !campaignValid && (
                <p className="text-xs text-amber-700 bg-amber-50 rounded-md px-2 py-1.5">
                  {t('sms.campaignRequired')}
                </p>
              )}
            </div>
          </>
        )}
      </CardContent>
    </Card>
  )
})
