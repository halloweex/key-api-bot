import { memo, useState, useCallback, useRef, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from './ChartContainer'
import { Badge } from './Badge'
import { Select } from './Select'
import { formatCurrency, formatNumber } from '../utils/formatters'
import { useTrafficUtmCampaigns } from '../hooks'
import { useQueryParams } from '../store/filterStore'
import type { UtmCampaignRow } from '../types/api'

// ─── Filter Options ──────────────────────────────────────────────────────────

const TRAFFIC_TYPES = [
  { value: '', labelKey: 'traffic.allTypes' },
  { value: 'paid_confirmed', labelKey: 'traffic.paid' },
  { value: 'paid_likely', labelKey: 'traffic.paidLikely' },
  { value: 'manager', labelKey: 'traffic.salesManager' },
  { value: 'organic', labelKey: 'traffic.organic' },
  { value: 'pixel_only', labelKey: 'traffic.pixelOnly' },
  { value: 'unknown', labelKey: 'traffic.unknown' },
] as const

const PLATFORMS = [
  { value: '', labelKey: 'traffic.allPlatforms' },
  { value: 'facebook', labelKey: 'traffic.facebook' },
  { value: 'instagram', labelKey: 'traffic.instagram' },
  { value: 'google', labelKey: 'traffic.google' },
  { value: 'tiktok', labelKey: 'traffic.tiktok' },
  { value: 'email', labelKey: 'traffic.email' },
  { value: 'telegram', labelKey: 'traffic.telegram' },
  { value: 'ai', labelKey: 'traffic.ai' },
  { value: 'manager', labelKey: 'traffic.manager' },
  { value: 'other', labelKey: 'traffic.otherPlatform' },
] as const

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

const FilterSelect = memo(function FilterSelect({
  value,
  onChange,
  options,
}: {
  value: string
  onChange: (v: string) => void
  options: readonly { value: string; labelKey: string }[]
}) {
  const { t } = useTranslation()
  const resolved = options.map((opt) => ({ value: opt.value, label: t(opt.labelKey) }))
  return (
    <Select
      options={resolved}
      value={value}
      onChange={(v) => onChange(v ?? options[0].value)}
      allowEmpty={false}
    />
  )
})

// ─── Sortable Header ─────────────────────────────────────────────────────────

type SortDir = 'asc' | 'desc'

// Columns where the first click should sort descending (biggest first)
const NUMERIC_COLUMNS = new Set(['orders', 'revenue'])

const SortableTh = memo(function SortableTh({
  column,
  label,
  sortBy,
  sortDir,
  onSort,
  align = 'left',
  className = '',
}: {
  column: string
  label: string
  sortBy: string
  sortDir: SortDir
  onSort: (column: string) => void
  align?: 'left' | 'right'
  className?: string
}) {
  const isActive = sortBy === column
  const arrow = isActive ? (sortDir === 'asc' ? '▲' : '▼') : ''

  return (
    <th
      className={`py-3 px-4 text-slate-600 font-semibold text-xs uppercase tracking-wide
                  cursor-pointer select-none hover:text-slate-900 hover:bg-slate-100 transition-colors
                  ${align === 'right' ? 'text-right' : 'text-left'} ${className}`}
      onClick={() => onSort(column)}
      aria-sort={isActive ? (sortDir === 'asc' ? 'ascending' : 'descending') : 'none'}
    >
      <span className="inline-flex items-center gap-1">
        {align === 'right' && isActive && <span className="text-[9px] text-blue-600">{arrow}</span>}
        {label}
        {align === 'left' && isActive && <span className="text-[9px] text-blue-600">{arrow}</span>}
      </span>
    </th>
  )
})

const PAGE_SIZE = 50

// ─── Main Component ──────────────────────────────────────────────────────────

export const UtmCampaignsTable = memo(function UtmCampaignsTable() {
  const { t } = useTranslation()
  const [trafficFilter, setTrafficFilter] = useState('')
  const [platformFilter, setPlatformFilter] = useState('')
  const [sortBy, setSortBy] = useState('revenue')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [offset, setOffset] = useState(0)
  // Accumulate loaded pages; keyed by the filter context to reset on change
  const pagesRef = useRef<UtmCampaignRow[]>([])
  const prevKeyRef = useRef('')

  const queryParams = useQueryParams()

  // Reset accumulated pages when global filters, local filters or sort change
  const resetKey = `${queryParams}|${trafficFilter}|${platformFilter}|${sortBy}|${sortDir}`
  if (resetKey !== prevKeyRef.current) {
    prevKeyRef.current = resetKey
    pagesRef.current = []
    if (offset !== 0) {
      setOffset(0)
    }
  }

  const { data, isLoading, error, refetch } = useTrafficUtmCampaigns(
    trafficFilter || null, platformFilter || null, sortBy, sortDir, PAGE_SIZE, offset,
  )

  const allCampaigns = useMemo(() => {
    if (!data?.campaigns) return pagesRef.current

    if (offset === 0) {
      pagesRef.current = data.campaigns
    } else if (pagesRef.current.length <= offset) {
      pagesRef.current = [...pagesRef.current, ...data.campaigns]
    }

    return pagesRef.current
  }, [data, offset])

  const handleTrafficChange = useCallback((value: string) => {
    setTrafficFilter(value)
    setOffset(0)
  }, [])

  const handlePlatformChange = useCallback((value: string) => {
    setPlatformFilter(value)
    setOffset(0)
  }, [])

  const handleSort = useCallback((column: string) => {
    setOffset(0)
    setSortBy(prev => {
      if (prev === column) {
        setSortDir(d => (d === 'asc' ? 'desc' : 'asc'))
        return prev
      }
      setSortDir(NUMERIC_COLUMNS.has(column) ? 'desc' : 'asc')
      return column
    })
  }, [])

  const handleLoadMore = useCallback(() => {
    setOffset(prev => prev + PAGE_SIZE)
  }, [])

  const total = data?.total ?? 0
  const hasMore = allCampaigns.length < total
  const isEmpty = !isLoading && allCampaigns.length === 0

  return (
    <ChartContainer
      title={t('traffic.utmCampaigns')}
      titleExtra={
        <div className="flex items-center gap-2">
          <FilterSelect value={trafficFilter} onChange={handleTrafficChange} options={TRAFFIC_TYPES} />
          <FilterSelect value={platformFilter} onChange={handlePlatformChange} options={PLATFORMS} />
        </div>
      }
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
              <SortableTh column="campaign" label={t('traffic.campaign')}
                sortBy={sortBy} sortDir={sortDir} onSort={handleSort} />
              <SortableTh column="utm_source" label={t('traffic.source')}
                sortBy={sortBy} sortDir={sortDir} onSort={handleSort} className="hidden md:table-cell" />
              <SortableTh column="platform" label={t('traffic.platform')}
                sortBy={sortBy} sortDir={sortDir} onSort={handleSort} className="hidden md:table-cell" />
              <SortableTh column="traffic_type" label={t('chart.type')}
                sortBy={sortBy} sortDir={sortDir} onSort={handleSort} />
              <SortableTh column="orders" label={t('common.orders')} align="right"
                sortBy={sortBy} sortDir={sortDir} onSort={handleSort} />
              <SortableTh column="revenue" label={t('common.revenue')} align="right"
                sortBy={sortBy} sortDir={sortDir} onSort={handleSort} />
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
