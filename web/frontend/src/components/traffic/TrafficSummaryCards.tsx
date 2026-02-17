import { memo, useState } from 'react'
import { StatCard, StatCardSkeleton } from '../cards/StatCard'
import { useTrafficAnalytics } from '../../hooks/useApi'
import { formatCurrency, formatNumber } from '../../utils/formatters'

// ─── Traffic Type Descriptions ────────────────────────────────────────────────

const TRAFFIC_DESCRIPTIONS = [
  {
    label: 'Paid Ads',
    color: 'bg-blue-500',
    description: 'Paid advertising orders (confirmed + likely).',
    details: [
      'Confirmed: explicit UTM (fbads, source=facebook/tiktok + medium=paid/cpc, Google CPC)',
      'Likely: _fbc cookie or fbclid present but no explicit UTM (previous ad click, 90-day cookie)',
    ],
  },
  {
    label: 'Organic',
    color: 'bg-green-500',
    description: 'Free traffic without paid advertising.',
    details: [
      'Social media: Instagram, Facebook, TikTok (utm_medium=social/organic)',
      'Email: Klaviyo, Rivo loyalty (utm_source=klaviyo/email/rivo)',
      'Google Shopping: utm_medium=product_sync (free listings)',
      'Instagram/Telegram: manager-created orders (source_id 1, 2)',
    ],
  },
  {
    label: 'Sales Manager',
    color: 'bg-cyan-500',
    description: 'Orders driven by sales managers.',
    details: [
      'Campaign starts with "sales_manager_" — human-driven sale',
    ],
  },
  {
    label: 'Pixel Only',
    color: 'bg-orange-500',
    description: 'Has tracking pixel but missing UTM parameters.',
    details: [
      'Facebook pixel (_fbp) or TikTok pixel (ttp) detected but no UTM',
      'Likely from ads, but cannot confirm — user may have returned directly',
    ],
  },
  {
    label: 'Unknown',
    color: 'bg-purple-500',
    description: 'No tracking data available.',
    details: [
      'Direct traffic: customer typed URL or used bookmark',
      'Untracked links: shared without UTM parameters',
      'Privacy tools: tracking blocked by browser/extensions',
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
        <h2 className="text-sm font-medium text-slate-600">Traffic Attribution</h2>
        <button
          onClick={() => setShowLegend(!showLegend)}
          className={`flex items-center gap-1.5 text-xs px-2 py-1 rounded-lg transition-colors ${
            showLegend
              ? 'bg-purple-100 text-purple-700'
              : 'text-slate-500 hover:text-slate-700 hover:bg-slate-100'
          }`}
        >
          <InfoIcon />
          <span>{showLegend ? 'Hide' : 'What is this?'}</span>
        </button>
      </div>

      {/* Legend */}
      {showLegend && (
        <div className="bg-slate-50 border border-slate-200 rounded-xl p-4 animate-fade-in">
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
            {TRAFFIC_DESCRIPTIONS.map((item) => (
              <div key={item.label} className="flex gap-3">
                <div className={`w-3 h-3 rounded-sm ${item.color} mt-1.5 flex-shrink-0`} />
                <div className="space-y-1">
                  <p className="text-sm font-semibold text-slate-700">{item.label}</p>
                  <p className="text-xs text-slate-600">{item.description}</p>
                  <ul className="text-xs text-slate-500 space-y-0.5 list-disc list-inside">
                    {item.details.map((detail, i) => (
                      <li key={i}>{detail}</li>
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
        label="Paid Ads"
        value={summary?.paid?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<AdsIcon />}
        variant="blue"
        subtitle={`${formatNumber(summary?.paid?.orders ?? 0)} orders`}
        ariaLabel={`Paid ads revenue: ${formatCurrency(summary?.paid?.revenue ?? 0)}`}
        clickable={!showPaidBreakdown}
        onClick={() => setShowPaidBreakdown(!showPaidBreakdown)}
      />
      <StatCard
        label="Organic"
        value={summary?.organic?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<LeafIcon />}
        variant="green"
        subtitle={`${formatNumber(summary?.organic?.orders ?? 0)} orders`}
        ariaLabel={`Organic revenue: ${formatCurrency(summary?.organic?.revenue ?? 0)}`}
      />
      <StatCard
        label="Sales Manager"
        value={summary?.manager?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<ManagerIcon />}
        variant="cyan"
        subtitle={`${formatNumber(summary?.manager?.orders ?? 0)} orders`}
        ariaLabel={`Sales manager revenue: ${formatCurrency(summary?.manager?.revenue ?? 0)}`}
      />
      <StatCard
        label="Pixel Only"
        value={summary?.pixel_only?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<PixelIcon />}
        variant="orange"
        subtitle={`${formatNumber(summary?.pixel_only?.orders ?? 0)} orders`}
        ariaLabel={`Pixel only revenue: ${formatCurrency(summary?.pixel_only?.revenue ?? 0)}`}
      />
      <StatCard
        label="Unknown"
        value={summary?.unknown?.revenue ?? 0}
        formatter={formatCurrency}
        icon={<QuestionIcon />}
        variant="purple"
        subtitle={`${formatNumber(summary?.unknown?.orders ?? 0)} orders`}
        ariaLabel={`Unknown source revenue: ${formatCurrency(summary?.unknown?.revenue ?? 0)}`}
      />
      </div>

      {/* Paid Ads Breakdown */}
      {showPaidBreakdown && (
        <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 animate-fade-in">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-semibold text-blue-800">Paid Ads Breakdown</h3>
            <button
              onClick={() => setShowPaidBreakdown(false)}
              className="text-blue-400 hover:text-blue-600 text-xs"
            >
              Close
            </button>
          </div>
          <div className="grid grid-cols-2 gap-3">
            <div className="bg-white rounded-lg p-3 border border-blue-100">
              <p className="text-xs text-slate-500 mb-1">Confirmed</p>
              <p className="text-lg font-bold text-blue-700">
                {formatCurrency(summary?.paid_confirmed?.revenue ?? 0)}
              </p>
              <p className="text-xs text-slate-500">
                {formatNumber(summary?.paid_confirmed?.orders ?? 0)} orders
              </p>
              <p className="text-xs text-blue-400 mt-1">Explicit UTM (fbads, cpc, paid)</p>
            </div>
            <div className="bg-white rounded-lg p-3 border border-blue-100">
              <p className="text-xs text-slate-500 mb-1">Likely</p>
              <p className="text-lg font-bold text-blue-500">
                {formatCurrency(summary?.paid_likely?.revenue ?? 0)}
              </p>
              <p className="text-xs text-slate-500">
                {formatNumber(summary?.paid_likely?.orders ?? 0)} orders
              </p>
              <p className="text-xs text-blue-400 mt-1">Cookie/fbclid only (no UTM)</p>
            </div>
          </div>
        </div>
      )}
    </div>
  )
})
