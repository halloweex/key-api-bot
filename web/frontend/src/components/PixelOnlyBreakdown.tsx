import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from './ChartContainer'
import { DataTable, Tr, Th, Td } from './DataTable'
import { InfoPopover } from './InfoPopover'
import { Wrapper } from './Wrapper'
import { useTrafficPixelOnly } from '../hooks/useApi'
import { formatCurrency, formatNumber, formatPercent } from '../utils/formatters'
import type { PixelOnlyBucket, PixelOnlyFirstChannel } from '../types/api'

// ─── Names ────────────────────────────────────────────────────────────────────
//
// An organic first channel is one of the organic breakdown's channels, an ad
// one is the tab's platform, and the rest name themselves. Platform names are
// proper nouns; the two that are not are said in the reader's language.

const GROUP_COLORS: Record<PixelOnlyFirstChannel['group'], string> = {
  organic: '#10B981',
  paid: '#3B82F6',
  manager: '#14B8A6',
  pixel_only: '#F97316',
  unknown: '#94A3B8',
}

function channelName(row: PixelOnlyFirstChannel, t: (key: string) => string): string {
  if (row.group === 'organic') return t(`traffic.organicChannel.${row.channel}`)
  if (row.group === 'paid') {
    const names: Record<string, string> = {
      facebook: 'Facebook', tiktok: 'TikTok', google_ads: 'Google Ads',
      instagram: 'Instagram', ai: 'AI', telegram: 'Telegram',
      other: t('traffic.otherPlatform'), unattributed: t('traffic.unattributedPlatform'),
    }
    return names[row.channel] ?? row.channel
  }
  return t(`traffic.firstChannel.${row.channel}`)
}

// ─── Pieces ───────────────────────────────────────────────────────────────────

const ShareBar = memo(function ShareBar({ pct, color }: { pct: number; color: string }) {
  return (
    <div className="flex items-center gap-2 min-w-[120px]">
      <div className="h-2 flex-1 rounded-full bg-slate-100 overflow-hidden">
        <div
          className="h-full rounded-full"
          style={{ width: `${Math.min(100, Math.max(0, pct))}%`, backgroundColor: color }}
        />
      </div>
      <span className="w-12 text-right tabular-nums text-slate-600">{formatPercent(pct)}</span>
    </div>
  )
})

const BucketSummary = memo(function BucketSummary({
  label, bucket, share, color,
}: { label: string; bucket: PixelOnlyBucket; share: number; color: string }) {
  const { t } = useTranslation()
  return (
    <div className="flex-1 min-w-[12rem] rounded-lg border border-slate-200 px-4 py-3">
      <div className="text-xs text-slate-500">{label}</div>
      <div className="text-lg font-semibold text-slate-800 tabular-nums">
        {formatCurrency(bucket.revenue)}
      </div>
      <div className="text-xs text-slate-500 tabular-nums">
        {t('traffic.pixelOnlyBucketDetail', {
          orders: formatNumber(bucket.orders), buyers: formatNumber(bucket.buyers),
        })}
      </div>
      <div className="mt-2"><ShareBar pct={share} color={color} /></div>
    </div>
  )
})

// ─── Component ────────────────────────────────────────────────────────────────

export const PixelOnlyBreakdown = memo(function PixelOnlyBreakdown() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useTrafficPixelOnly()

  const total = data?.totals.revenue ?? 0
  const pct = (revenue: number) => (total > 0 ? (revenue / total) * 100 : 0)
  const isEmpty = !isLoading && (data?.totals.orders ?? 0) === 0

  return (
    <ChartContainer
      title={t('traffic.pixelOnlyBreakdown')}
      titleExtra={
        <InfoPopover title={t('traffic.pixelOnlyHowTitle')} size="wide">
          <p className="text-xs text-slate-300 leading-relaxed">{t('traffic.pixelOnlyHowText')}</p>
        </InfoPopover>
      }
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      height="auto"
      isEmpty={isEmpty}
      emptyMessage={t('traffic.pixelOnlyEmpty')}
      ariaLabel={t('traffic.pixelOnlyBreakdownDesc')}
    >
      {data && (
        <Wrapper dir="column" gap="md">
          <Wrapper dir="row-responsive" gap="md">
            <BucketSummary
              label={t('traffic.pixelOnlyNew')}
              bucket={data.new}
              share={pct(data.new.revenue)}
              color={GROUP_COLORS.pixel_only}
            />
            <BucketSummary
              label={t('traffic.pixelOnlyReturning')}
              bucket={data.returning}
              share={pct(data.returning.revenue)}
              color={GROUP_COLORS.organic}
            />
          </Wrapper>

          <DataTable variant="feature">
            <thead>
              <Tr header>
                <Th>{t('traffic.pixelOnlyColFirst')}</Th>
                <Th align="right">{t('traffic.pixelOnlyColBuyers')}</Th>
                <Th align="right" hideBelow="md">{t('traffic.pixelOnlyColNew')}</Th>
                <Th align="right" hideBelow="sm">{t('traffic.organicColOrders')}</Th>
                <Th align="right">{t('traffic.organicColRevenue')}</Th>
                <Th hideBelow="sm">{t('traffic.pixelOnlyColShare')}</Th>
              </Tr>
            </thead>
            <tbody>
              {data.first_channels.map((row) => (
                <Tr key={`${row.group}:${row.channel}`}>
                  <Td bold>
                    <span className="inline-flex items-center gap-2">
                      <span
                        className="w-2.5 h-2.5 rounded-full shrink-0"
                        style={{ backgroundColor: GROUP_COLORS[row.group] ?? GROUP_COLORS.unknown }}
                        aria-hidden="true"
                      />
                      <span className="flex flex-col">
                        <span>{channelName(row, t)}</span>
                        <span className="text-xs font-normal text-slate-400">
                          {t(`traffic.pixelOnlyGroup.${row.group}`)}
                        </span>
                      </span>
                    </span>
                  </Td>
                  <Td align="right" tabular>{formatNumber(row.buyers)}</Td>
                  <Td align="right" tabular hideBelow="md">{formatNumber(row.new_buyers)}</Td>
                  <Td align="right" tabular hideBelow="sm">{formatNumber(row.orders)}</Td>
                  <Td align="right" tabular>{formatCurrency(row.revenue)}</Td>
                  <Td hideBelow="sm">
                    <ShareBar pct={pct(row.revenue)} color={GROUP_COLORS[row.group] ?? GROUP_COLORS.unknown} />
                  </Td>
                </Tr>
              ))}
            </tbody>
          </DataTable>
        </Wrapper>
      )}
    </ChartContainer>
  )
})

export default PixelOnlyBreakdown
