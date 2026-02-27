import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Megaphone, Sparkles, Monitor, CircleHelp, User, Info } from 'lucide-react'
import { StatCard, StatCardSkeleton } from '../cards/StatCard'
import { useTrafficAnalytics } from '../../hooks/useApi'
import { formatCurrency, formatNumber } from '../../utils/formatters'

// ─── Traffic Type Descriptions ────────────────────────────────────────────────

const TRAFFIC_DESCRIPTIONS = [
  {
    labelKey: 'traffic.paidAds',
    color: 'bg-blue-500',
    descriptionKey: 'traffic.paidAdsDesc',
    detailKeys: [
      'traffic.paidConfirmedDesc',
      'traffic.paidLikelyDesc',
    ],
  },
  {
    labelKey: 'traffic.organic',
    color: 'bg-green-500',
    descriptionKey: 'traffic.organicDesc',
    detailKeys: [
      'traffic.organicSocialDesc',
      'traffic.organicEmailDesc',
      'traffic.organicShoppingDesc',
      'traffic.organicManagerDesc',
    ],
  },
  {
    labelKey: 'traffic.salesManager',
    color: 'bg-cyan-500',
    descriptionKey: 'traffic.salesManagerDesc',
    detailKeys: [
      'traffic.salesManagerCampaign',
    ],
  },
  {
    labelKey: 'traffic.pixelOnly',
    color: 'bg-orange-500',
    descriptionKey: 'traffic.pixelOnlyDesc',
    detailKeys: [
      'traffic.pixelOnlyDetail',
      'traffic.pixelOnlyNote',
    ],
  },
  {
    labelKey: 'traffic.unknown',
    color: 'bg-purple-500',
    descriptionKey: 'traffic.unknownDesc',
    detailKeys: [
      'traffic.unknownDirectDesc',
      'traffic.unknownUntrackedDesc',
      'traffic.unknownPrivacyDesc',
    ],
  },
]

// ─── Component ────────────────────────────────────────────────────────────────

export const TrafficSummaryCards = memo(function TrafficSummaryCards() {
  const { t } = useTranslation()
  const { data, isLoading } = useTrafficAnalytics()
  const [showLegend, setShowLegend] = useState(false)
  const [showPaidBreakdown, setShowPaidBreakdown] = useState(false)

  if (isLoading) {
    return (
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-3 sm:gap-4">
        <StatCardSkeleton />
        <StatCardSkeleton />
        <StatCardSkeleton />
        <StatCardSkeleton />
        <StatCardSkeleton />
      </div>
    )
  }

  const summary = data?.summary

  return (
    <div className="space-y-3">
      {/* Header with info button */}
      <div className="flex items-center justify-between">
        <h2 className="text-base font-semibold text-slate-800 tracking-tight">{t('traffic.attribution')}</h2>
        <button
          onClick={() => setShowLegend(!showLegend)}
          className={`flex items-center gap-1.5 text-xs px-2 py-1 rounded-lg transition-colors ${
            showLegend
              ? 'bg-purple-100 text-purple-700'
              : 'text-slate-500 hover:text-slate-700 hover:bg-slate-100'
          }`}
        >
          <Info className="w-4 h-4" />
          <span>{showLegend ? t('traffic.hide') : t('traffic.whatIsThis')}</span>
        </button>
      </div>

      {/* Legend */}
      {showLegend && (
        <div className="bg-slate-50 border border-slate-200 rounded-xl p-4 animate-fade-in">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {TRAFFIC_DESCRIPTIONS.map((item) => (
              <div key={item.labelKey} className="flex gap-3">
                <div className={`w-3 h-3 rounded-sm ${item.color} mt-1.5 flex-shrink-0`} />
                <div className="space-y-1">
                  <p className="text-sm font-semibold text-slate-700">{t(item.labelKey)}</p>
                  <p className="text-xs text-slate-600">{t(item.descriptionKey)}</p>
                  <ul className="text-xs text-slate-500 space-y-0.5 list-disc list-inside">
                    {item.detailKeys.map((detailKey, i) => (
                      <li key={i}>{t(detailKey)}</li>
                    ))}
                  </ul>
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Cards */}
      <div className="grid grid-cols-2 lg:grid-cols-5 gap-3 sm:gap-4">
      <StatCard
        label={t('traffic.paidAds')}
        value={summary?.paid?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<Megaphone className="w-5 h-5" />}
        variant="blue"
        subtitle={`${formatNumber(summary?.paid?.orders ?? 0)} ${t('common.orders')}`}
        ariaLabel={`${t('traffic.paidAds')}: ${formatCurrency(summary?.paid?.revenue ?? 0)}`}
        clickable={!showPaidBreakdown}
        onClick={() => setShowPaidBreakdown(!showPaidBreakdown)}
      />
      <StatCard
        label={t('traffic.organic')}
        value={summary?.organic?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<Sparkles className="w-5 h-5" />}
        variant="green"
        subtitle={`${formatNumber(summary?.organic?.orders ?? 0)} ${t('common.orders')}`}
        ariaLabel={`${t('traffic.organic')}: ${formatCurrency(summary?.organic?.revenue ?? 0)}`}
      />
      <StatCard
        label={t('traffic.salesManager')}
        value={summary?.manager?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<User className="w-5 h-5" />}
        variant="cyan"
        subtitle={`${formatNumber(summary?.manager?.orders ?? 0)} ${t('common.orders')}`}
        ariaLabel={`${t('traffic.salesManager')}: ${formatCurrency(summary?.manager?.revenue ?? 0)}`}
      />
      <StatCard
        label={t('traffic.pixelOnly')}
        value={summary?.pixel_only?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<Monitor className="w-5 h-5" />}
        variant="orange"
        subtitle={`${formatNumber(summary?.pixel_only?.orders ?? 0)} ${t('common.orders')}`}
        ariaLabel={`${t('traffic.pixelOnly')}: ${formatCurrency(summary?.pixel_only?.revenue ?? 0)}`}
      />
      <StatCard
        label={t('traffic.unknown')}
        value={summary?.unknown?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<CircleHelp className="w-5 h-5" />}
        variant="purple"
        subtitle={`${formatNumber(summary?.unknown?.orders ?? 0)} ${t('common.orders')}`}
        ariaLabel={`${t('traffic.unknown')}: ${formatCurrency(summary?.unknown?.revenue ?? 0)}`}
      />
      </div>

      {/* Paid Ads Breakdown */}
      {showPaidBreakdown && (
        <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 animate-fade-in">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-blue-800">{t('traffic.paidAdsBreakdown')}</h3>
            <button
              onClick={() => setShowPaidBreakdown(false)}
              className="text-blue-400 hover:text-blue-600 text-xs"
            >
              {t('traffic.close')}
            </button>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="bg-white rounded-lg p-3 border border-blue-100">
              <p className="text-xs text-slate-500 mb-1">{t('traffic.confirmed')}</p>
              <p className="text-lg font-bold text-blue-700">
                {formatCurrency(summary?.paid_confirmed?.revenue ?? 0)}
              </p>
              <p className="text-xs text-slate-500">
                {formatNumber(summary?.paid_confirmed?.orders ?? 0)} {t('common.orders')}
              </p>
              <p className="text-xs text-blue-400 mt-1">{t('traffic.confirmedDesc')}</p>
            </div>
            <div className="bg-white rounded-lg p-3 border border-blue-100">
              <p className="text-xs text-slate-500 mb-1">{t('traffic.likely')}</p>
              <p className="text-lg font-bold text-blue-500">
                {formatCurrency(summary?.paid_likely?.revenue ?? 0)}
              </p>
              <p className="text-xs text-slate-500">
                {formatNumber(summary?.paid_likely?.orders ?? 0)} {t('common.orders')}
              </p>
              <p className="text-xs text-blue-400 mt-1">{t('traffic.likelyDesc')}</p>
            </div>
          </div>
        </div>
      )}
    </div>
  )
})
