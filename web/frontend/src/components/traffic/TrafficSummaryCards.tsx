import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
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

// ─── Icons ────────────────────────────────────────────────────────────────────

const AdsIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M15 10l4.553-2.276A1 1 0 0121 8.618v6.764a1 1 0 01-1.447.894L15 14M5 18h8a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z" />
  </svg>
)

const LeafIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M5 3v4M3 5h4M6 17v4m-2-2h4m5-16l2.286 6.857L21 12l-5.714 2.143L13 21l-2.286-6.857L5 12l5.714-2.143L13 3z" />
  </svg>
)

const PixelIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z" />
  </svg>
)

const QuestionIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M8.228 9c.549-1.165 2.03-2 3.772-2 2.21 0 4 1.343 4 3 0 1.4-1.278 2.575-3.006 2.907-.542.104-.994.54-.994 1.093m0 3h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
  </svg>
)

const ManagerIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M16 7a4 4 0 11-8 0 4 4 0 018 0zM12 14a7 7 0 00-7 7h14a7 7 0 00-7-7z" />
  </svg>
)

const InfoIcon = () => (
  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
  </svg>
)

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
          <InfoIcon />
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
        icon={<AdsIcon />}
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
        icon={<LeafIcon />}
        variant="green"
        subtitle={`${formatNumber(summary?.organic?.orders ?? 0)} ${t('common.orders')}`}
        ariaLabel={`${t('traffic.organic')}: ${formatCurrency(summary?.organic?.revenue ?? 0)}`}
      />
      <StatCard
        label={t('traffic.salesManager')}
        value={summary?.manager?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<ManagerIcon />}
        variant="cyan"
        subtitle={`${formatNumber(summary?.manager?.orders ?? 0)} ${t('common.orders')}`}
        ariaLabel={`${t('traffic.salesManager')}: ${formatCurrency(summary?.manager?.revenue ?? 0)}`}
      />
      <StatCard
        label={t('traffic.pixelOnly')}
        value={summary?.pixel_only?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<PixelIcon />}
        variant="orange"
        subtitle={`${formatNumber(summary?.pixel_only?.orders ?? 0)} ${t('common.orders')}`}
        ariaLabel={`${t('traffic.pixelOnly')}: ${formatCurrency(summary?.pixel_only?.revenue ?? 0)}`}
      />
      <StatCard
        label={t('traffic.unknown')}
        value={summary?.unknown?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<QuestionIcon />}
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
