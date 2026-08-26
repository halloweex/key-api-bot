import { memo, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { Badge } from './Badge'
import { Card, CardContent } from './Card'
import { SkeletonCard } from './Skeleton'
import { SmsSelectionCriteria } from './SmsSelectionCriteria'
import { formatCurrency, formatNumber } from '../utils/formatters'
import type { SmsSegment, SmsSegmentsResponse, SmsTier } from '../types/api'

// ─── SmsAudiencePreview ──────────────────────────────────────────────────────
//
// Who is in the audience right now, under the filters currently set.
//
// Every arm states the rule that built it, in the numbers actually in force
// rather than in prose: an arm whose membership cannot be explained is one
// nobody trusts enough to spend budget against. Under a filtered audience
// there is one arm and the rule is the filters themselves, so the card says
// what it is instead of pretending to a tier definition it does not have.
//
// The fuller account — what each rule removed, in order — sits under
// <SmsSelectionCriteria>.

const TIER_STYLE: Record<SmsTier, { tone: 'purple' | 'blue' | 'orange' | 'green'; order: number }> = {
  VIP: { tone: 'purple', order: 0 },
  CORE: { tone: 'blue', order: 1 },
  REACTIVATION: { tone: 'orange', order: 2 },
  ALL: { tone: 'green', order: 0 },
}

/** The membership rule for an arm, in the thresholds actually applied. */
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
    case 'ALL':
      return t('sms.ruleSingleGroup', { days: criteria.maxRecencyDays })
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

export const SmsAudiencePreview = memo(function SmsAudiencePreview({
  data,
  isLoading,
}: {
  data?: SmsSegmentsResponse
  isLoading?: boolean
}) {
  const { t } = useTranslation()

  const segments = useMemo(
    () =>
      [...(data?.segments ?? [])].sort(
        (a, b) => (TIER_STYLE[a.tier]?.order ?? 9) - (TIER_STYLE[b.tier]?.order ?? 9),
      ),
    [data],
  )

  if (isLoading) {
    return (
      <div className="grid gap-3 sm:grid-cols-3">
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
      </div>
    )
  }

  if (!data) return null

  // An audience nobody can be sent to is the one case where the numbers say
  // nothing and the funnel says everything, so lead with the funnel.
  const empty = data.totals.customers === 0

  return (
    <>
      {empty ? (
        <div className="rounded-md bg-amber-50 px-3 py-2 text-xs text-amber-800">
          {t('sms.audienceEmpty')}
        </div>
      ) : (
        <div
          className={`grid gap-3 ${segments.length > 1 ? 'sm:grid-cols-3' : 'sm:grid-cols-1'}`}
        >
          {segments.map((s) => (
            <TierCard key={s.tier} segment={s} criteria={data.criteria} />
          ))}
        </div>
      )}

      <div className="mt-3 text-xs text-slate-500 tabular-nums">
        {t('sms.totals', {
          total: formatNumber(data.totals.customers),
          target: formatNumber(data.totals.target),
          holdout: formatNumber(data.totals.holdout),
        })}
      </div>

      <SmsSelectionCriteria data={data} />
    </>
  )
})
