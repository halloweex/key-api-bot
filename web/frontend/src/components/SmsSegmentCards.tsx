import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Card, CardContent, CardHeader, CardTitle } from './Card'
import { Button } from './Button'
import { Checkbox } from './Checkbox'
import { Select } from './Select'
import { Badge } from './Badge'
import { ApiErrorState } from './ApiErrorState'
import { SkeletonCard } from './Skeleton'
import { SmsSelectionCriteria } from './SmsSelectionCriteria'
import { SmsTestSendDialog } from './SmsTestSendDialog'
import { useSmsSegments } from '../hooks/useApi'
import { formatCurrency, formatNumber } from '../utils/formatters'
import type { SmsLtvBasis, SmsSegment, SmsSegmentsResponse, SmsTier } from '../types/api'

// ─── SmsSegmentCards ─────────────────────────────────────────────────────────
//
// Pick the criteria, see the three tiers, take the file.
//
// Every tier states the rule that built it, in the numbers currently in force
// rather than in prose: a tier whose membership cannot be explained is a tier
// nobody trusts enough to spend budget against. The fuller account — what the
// rules removed, and in what order — sits under <SmsSelectionCriteria>.
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

/** The membership rule for a tier, in the thresholds actually applied. */
function tierRule(
  tier: SmsTier,
  criteria: SmsSegmentsResponse['criteria'],
  t: (key: string, opts?: Record<string, unknown>) => string,
): string {
  switch (tier) {
    case 'VIP':
      return t('sms.ruleLtvAtLeast', { value: formatCurrency(criteria.vipLtv) })
    case 'CORE':
      return t('sms.ruleCoreValue', {
        orders: criteria.coreMinOrders,
        value: formatCurrency(criteria.coreLtv),
      })
    default:
      return t('sms.ruleReactivationValue', { days: criteria.reactivationMaxRecency })
  }
}

function TierCard({
  segment,
  criteria,
}: {
  segment: SmsSegment
  criteria: SmsSegmentsResponse['criteria']
}) {
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

        <p className="mt-2 text-[11px] text-slate-500 leading-snug">
          {tierRule(segment.tier, criteria, t)}
        </p>

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
  const [testing, setTesting] = useState(false)
  // Which tiers this campaign is for. A discount suits Core and Reactivation
  // and cannibalises VIP, so the roster has to be selectable — and empty means
  // all three, matching what the cards above show.
  const [pickedTiers, setPickedTiers] = useState<SmsTier[]>([])
  // How much of each tier is withheld. The API has always accepted 0–50, but
  // the page never offered it, so every campaign went out at the default 10 —
  // and at 10 a tier cannot show anything short of a 6–12 pp effect, which is
  // not an effect anyone gets. Precision is bought by withholding more, not by
  // sending more, so this is the one control on the page that decides whether
  // the campaign will be measurable at all.
  const [holdoutPct, setHoldoutPct] = useState(10)

  const params = useMemo(
    () => `ltv_basis=${ltvBasis}&holdout_pct=${holdoutPct}&include_customers=false`,
    [ltvBasis, holdoutPct],
  )
  const { data, isLoading, error, refetch } = useSmsSegments(params)

  const campaignValid = CAMPAIGN_PATTERN.test(campaign)
  // An unnamed export is legitimate (a look at the list); a badly named one is
  // a typo that would otherwise download an anonymous file and look like it
  // worked. Freezing needs the name either way.
  const canExport = campaign.length === 0 ? !freeze : campaignValid

  const segments = useMemo(
    () =>
      [...(data?.segments ?? [])].sort(
        (a, b) => (TIER_STYLE[a.tier]?.order ?? 9) - (TIER_STYLE[b.tier]?.order ?? 9),
      ),
    [data],
  )

  function toggleTier(tier: SmsTier) {
    setPickedTiers((prev) =>
      prev.includes(tier) ? prev.filter((t) => t !== tier) : [...prev, tier],
    )
  }

  function handleExport() {
    const p = new URLSearchParams({
      ltv_basis: ltvBasis,
      holdout_pct: String(holdoutPct),
    })
    if (campaignValid) p.set('campaign', campaign)
    if (freeze) p.set('freeze', 'true')
    if (promocode.trim()) p.set('promocode', promocode.trim())
    // Nothing picked means every tier, so the parameter is left off entirely
    // rather than sent empty.
    if (pickedTiers.length > 0) p.set('tier', pickedTiers.join(','))
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
            <div className="flex items-center gap-2">
              <Badge tone="slate" shape="tag">{t('sms.step', { n: 1 })}</Badge>
              <CardTitle>{t('sms.segmentsTitle')}</CardTitle>
            </div>
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
            {data && (
              <div className="grid gap-3 sm:grid-cols-3">
                {segments.map((s) => (
                  <TierCard key={s.tier} segment={s} criteria={data.criteria} />
                ))}
              </div>
            )}

            <div className="mt-3 text-xs text-slate-500 tabular-nums">
              {t('sms.totals', {
                total: formatNumber(data?.totals.customers ?? 0),
                target: formatNumber(data?.totals.target ?? 0),
                holdout: formatNumber(data?.totals.holdout ?? 0),
              })}
            </div>

            {data && <SmsSelectionCriteria data={data} />}

            {/* ── Export ─────────────────────────────────────────────── */}
            <div className="mt-4 pt-4 border-t border-slate-100">
              <h3 className="text-sm font-medium text-slate-700">{t('sms.exportTitle')}</h3>
              <p className="text-xs text-slate-500 mt-0.5 mb-3">{t('sms.exportDesc')}</p>

              <div className="space-y-3">
                {/* ── Who the campaign is for ──────────────────────── */}
                <div>
                  <span className="text-xs text-slate-600">{t('sms.exportTiers')}</span>
                  <div
                    className="mt-1 flex flex-wrap gap-2"
                    role="group"
                    aria-label={t('sms.exportTiers')}
                  >
                    {segments.map((s) => {
                      const on = pickedTiers.includes(s.tier)
                      return (
                        <button
                          key={s.tier}
                          type="button"
                          aria-pressed={on}
                          onClick={() => toggleTier(s.tier)}
                          className={`px-3 py-1.5 text-xs rounded-md border transition-colors
                                      tabular-nums ${
                            on
                              ? 'border-purple-400 bg-purple-50 text-purple-800 font-medium'
                              : 'border-slate-200 text-slate-600 hover:border-slate-300'
                          }`}
                        >
                          {t(`sms.tier.${s.tier}`)} · {formatNumber(s.target)}
                        </button>
                      )
                    })}
                  </div>
                  <p className="mt-1 text-[11px] text-slate-500">
                    {pickedTiers.length === 0
                      ? t('sms.exportAllTiers', {
                          total: formatNumber(data?.totals.target ?? 0),
                        })
                      : t('sms.exportPickedTiers', {
                          total: formatNumber(
                            segments
                              .filter((s) => pickedTiers.includes(s.tier))
                              .reduce((n, s) => n + s.target, 0),
                          ),
                        })}
                  </p>
                </div>

                {/* ── How much to withhold ─────────────────────────── */}
                <div>
                  <span className="text-xs text-slate-600">{t('sms.holdoutLabel')}</span>
                  <div
                    className="mt-1 flex flex-wrap gap-2"
                    role="group"
                    aria-label={t('sms.holdoutLabel')}
                  >
                    {[10, 20, 30, 40].map((pct) => (
                      <button
                        key={pct}
                        type="button"
                        aria-pressed={holdoutPct === pct}
                        onClick={() => setHoldoutPct(pct)}
                        className={`px-3 py-1.5 text-xs rounded-md border transition-colors
                                    tabular-nums ${
                          holdoutPct === pct
                            ? 'border-purple-400 bg-purple-50 text-purple-800 font-medium'
                            : 'border-slate-200 text-slate-600 hover:border-slate-300'
                        }`}
                      >
                        {pct}%
                      </button>
                    ))}
                  </div>
                  <p className="mt-1 text-[11px] text-slate-500 leading-snug">
                    {t('sms.holdoutHint', {
                      target: formatNumber(data?.totals.target ?? 0),
                      holdout: formatNumber(data?.totals.holdout ?? 0),
                    })}
                  </p>
                </div>

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
                  {/* Rehearsing the text belongs before the roster is frozen —
                      once a campaign is sent the wording cannot be taken back. */}
                  <Button variant="secondary" size="sm" onClick={() => setTesting(true)}>
                    {t('sms.testSend')}
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

                {/* A name that fails the pattern is dropped from the request
                    rather than rejected, so say so before the file lands
                    unnamed — not only when freeze forces the issue. */}
                {campaign.length > 0 && !campaignValid && (
                  <p className="text-xs text-amber-700 bg-amber-50 rounded-md px-2 py-1.5">
                    {t('sms.campaignRequired')}
                  </p>
                )}
              </div>
            </div>
          </>
        )}
      </CardContent>

      {testing && <SmsTestSendDialog onClose={() => setTesting(false)} />}
    </Card>
  )
})
