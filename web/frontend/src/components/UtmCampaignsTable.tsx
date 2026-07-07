import { memo, useState, useCallback, useRef, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from './ChartContainer'
import { Badge } from './Badge'
import { formatCurrency, formatNumber } from '../utils/formatters'
import { useTrafficUtmCampaigns } from '../hooks'
import { useQueryParams } from '../store/filterStore'
import type { UtmCampaignRow } from '../types/api'

// ─── Traffic Type Badges ─────────────────────────────────────────────────────

type BadgeTone = 'neutral' | 'green' | 'red' | 'blue' | 'purple' | 'orange' | 'cyan'

const trafficBadgeConfig: Record<string, { tone: BadgeTone; labelKey: string }> = {
  paid_confirmed: { tone: 'blue', labelKey: 'traffic.paid' },
  paid_likely:    { tone: 'blue', labelKey: 'traffic.paidLikely' },
  manager:        { tone: 'cyan', labelKey: 'traffic.manager' },
  organic:        { tone: 'green', labelKey: 'traffic.organic' },
  pixel_only:     { tone: 'orange', labelKey: 'traffic.pixelOnly' },
  unknown:        { tone: 'purple', labelKey: 'traffic.unknown' },
}

const TrafficBadge = memo(function TrafficBadge({ type }: { type: string }) {
  const { t } = useTranslation()
  const config = trafficBadgeConfig[type] || trafficBadgeConfig.unknown
  return <Badge tone={config.tone}>{t(config.labelKey)}</Badge>
})

const formatPlatformName = (platform: string, trafficType: string): string => {
  if (platform === 'google') {
    return trafficType === 'paid_confirmed' || trafficType === 'paid_likely'
      ? 'Google Ads'
      : 'Google Organic'
  }
  const names: Record<string, string> = {
    facebook: 'Facebook',
    tiktok: 'TikTok',
    instagram: 'Instagram',
    email: 'Email',
    telegram: 'Telegram',
    ai: 'AI',
    manager: 'Manager',
  }
  return names[platform] || platform.charAt(0).toUpperCase() + platform.slice(1)
}

const PAGE_SIZE = 50

// ─── Main Component ──────────────────────────────────────────────────────────

export const UtmCampaignsTable = memo(function UtmCampaignsTable() {
  const { t } = useTranslation()
  const [offset, setOffset] = useState(0)
  // Accumulate loaded pages; keyed by the filter context to reset on change
  const pagesRef = useRef<UtmCampaignRow[]>([])
  const prevKeyRef = useRef('')

  const queryParams = useQueryParams()

  // Reset accumulated pages when global filters change
  if (queryParams !== prevKeyRef.current) {
    prevKeyRef.current = queryParams
    pagesRef.current = []
    if (offset !== 0) {
      setOffset(0)
    }
  }

  const { data, isLoading, error, refetch } = useTrafficUtmCampaigns(PAGE_SIZE, offset)

  const allCampaigns = useMemo(() => {
    if (!data?.campaigns) return pagesRef.current

    if (offset === 0) {
      pagesRef.current = data.campaigns
    } else if (pagesRef.current.length <= offset) {
      pagesRef.current = [...pagesRef.current, ...data.campaigns]
    }

    return pagesRef.current
  }, [data, offset])

  const handleLoadMore = useCallback(() => {
    setOffset(prev => prev + PAGE_SIZE)
  }, [])

  const total = data?.total ?? 0
  const hasMore = allCampaigns.length < total
  const isEmpty = !isLoading && allCampaigns.length === 0

  return (
    <ChartContainer
      title={t('traffic.utmCampaigns')}
      isLoading={isLoading && offset === 0}
      error={error as Error | null}
      onRetry={refetch}
      height="auto"
      isEmpty={isEmpty}
      emptyMessage={t('traffic.noUtmData')}
      ariaLabel={t('traffic.utmCampaignsDesc')}
    >
      <div className="overflow-auto rounded-lg border border-slate-200 max-h-[500px]">
        <table className="w-full text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="bg-slate-50 border-b border-slate-200">
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                {t('traffic.campaign')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden md:table-cell">
                {t('traffic.source')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide hidden md:table-cell">
                {t('traffic.platform')}
              </th>
              <th className="text-left py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                {t('chart.type')}
              </th>
              <th className="text-right py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                {t('common.orders')}
              </th>
              <th className="text-right py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide">
                {t('common.revenue')}
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {allCampaigns.map((row, i) => (
              <tr
                key={`${row.campaign}|${row.platform}|${row.traffic_type}|${i}`}
                className="hover:bg-slate-50 transition-colors"
              >
                <td className="py-3 px-4">
                  {row.campaign ? (
                    <span
                      className="text-xs text-slate-800 font-medium max-w-[280px] truncate block"
                      title={row.campaign}
                    >
                      {row.campaign}
                    </span>
                  ) : (
                    <span className="text-xs text-slate-400 italic">{t('traffic.noUtm')}</span>
                  )}
                </td>
                <td className="py-3 px-4 hidden md:table-cell">
                  <span
                    className="text-xs text-slate-500 max-w-[160px] truncate block"
                    title={row.utm_source ?? undefined}
                  >
                    {row.utm_source || '--'}
                  </span>
                </td>
                <td className="py-3 px-4 text-slate-600 hidden md:table-cell whitespace-nowrap">
                  {formatPlatformName(row.platform, row.traffic_type)}
                </td>
                <td className="py-3 px-4">
                  <TrafficBadge type={row.traffic_type} />
                </td>
                <td className="py-3 px-4 text-right text-slate-600 whitespace-nowrap">
                  {formatNumber(row.orders)}
                </td>
                <td className="py-3 px-4 text-right text-slate-800 font-semibold whitespace-nowrap">
                  {formatCurrency(row.revenue)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Footer: count + load more */}
      <div className="flex items-center justify-between mt-3 px-1">
        <span className="text-xs text-slate-400">
          {t('traffic.showing')} {allCampaigns.length} {t('common.of')} {total}
        </span>
        {hasMore && (
          <button
            onClick={handleLoadMore}
            disabled={isLoading}
            className="text-xs px-3 py-1.5 rounded-lg bg-slate-100 text-slate-600 hover:bg-slate-200
                       transition-colors disabled:opacity-50 disabled:cursor-not-allowed font-medium"
          >
            {isLoading ? t('common.loading') : t('traffic.loadMore')}
          </button>
        )}
      </div>
    </ChartContainer>
  )
})
