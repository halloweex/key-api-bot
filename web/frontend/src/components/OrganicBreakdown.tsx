import { Fragment, memo, useCallback, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { ChartContainer } from './ChartContainer'
import { DataTable, Tr, Th, Td } from './DataTable'
import { InfoPopover } from './InfoPopover'
import { Wrapper } from './Wrapper'
import { useTrafficAnalytics, useTrafficOrganic } from '../hooks/useApi'
import { formatCurrency, formatDate, formatNumber, formatPercent } from '../utils/formatters'
import type { OrganicChannel, OrganicChannelKey } from '../types/api'

// ─── Channels ─────────────────────────────────────────────────────────────────
//
// The order is the server's (`ORGANIC_CHANNELS` in core/utm_classify.py) and
// is what the explanation lists. Colours keep each family together — the
// Instagram channels in pinks, email in greens — so the share bars read as
// "mostly Instagram" before anyone reads a label.

const CHANNELS: OrganicChannelKey[] = [
  'instagram_direct', 'instagram_site', 'instagram_stories', 'telegram',
  'email_campaigns', 'email_flows', 'google_organic', 'ai',
  'facebook', 'tiktok', 'other',
]

const CHANNEL_COLORS: Record<OrganicChannelKey, string> = {
  instagram_direct: '#DB2777',
  instagram_site: '#EC4899',
  instagram_stories: '#F9A8D4',
  telegram: '#0EA5E9',
  email_campaigns: '#059669',
  email_flows: '#34D399',
  google_organic: '#93C5FD',
  ai: '#A855F7',
  facebook: '#7C3AED',
  tiktok: '#06B6D4',
  other: '#94A3B8',
}

const colorOf = (channel: string): string =>
  CHANNEL_COLORS[channel as OrganicChannelKey] ?? CHANNEL_COLORS.other

// ─── Change against the previous period ───────────────────────────────────────

type Change =
  | { kind: 'pct'; value: number }
  | { kind: 'new' }
  | { kind: 'none' }

function changeOf(current: number, previous: number): Change {
  if (previous > 0) return { kind: 'pct', value: ((current - previous) / previous) * 100 }
  return current > 0 ? { kind: 'new' } : { kind: 'none' }
}

function changeText(change: Change, t: (key: string) => string): string {
  if (change.kind === 'new') return t('traffic.organicNewChannel')
  if (change.kind === 'none') return '—'
  const sign = change.value > 0.05 ? '+' : change.value < -0.05 ? '−' : ''
  return `${sign}${formatPercent(Math.abs(change.value))}`
}

const ChangeValue = memo(function ChangeValue({ change }: { change: Change }) {
  const { t } = useTranslation()
  const tone =
    change.kind === 'none' ? 'text-slate-400'
      : change.kind === 'new' ? 'text-green-600 font-medium'
        : change.value > 0.05 ? 'text-green-600 font-medium'
          : change.value < -0.05 ? 'text-red-600 font-medium'
            : 'text-slate-500 font-medium'
  return <span className={tone}>{changeText(change, t)}</span>
})

// ─── Share bar ────────────────────────────────────────────────────────────────

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

// ─── Rows ─────────────────────────────────────────────────────────────────────

const ChannelRows = memo(function ChannelRows({
  row,
  organicRevenue,
  open,
  onToggle,
}: {
  row: OrganicChannel
  organicRevenue: number
  open: boolean
  onToggle: (channel: string) => void
}) {
  const { t } = useTranslation()
  const name = t(`traffic.organicChannel.${row.channel}`)
  const share = organicRevenue > 0 ? (row.revenue / organicRevenue) * 100 : 0
  const expandable = row.details_total > 0
  const more = row.details_total - row.details.length

  return (
    <Fragment>
      <Tr onClick={expandable ? () => onToggle(row.channel) : undefined}>
        <Td bold>
          <span className="inline-flex items-center gap-2">
            {expandable ? (
              <button
                type="button"
                onClick={(e) => { e.stopPropagation(); onToggle(row.channel) }}
                aria-expanded={open}
                aria-label={t('traffic.organicShowDetails', { value: name })}
                className="text-slate-400 hover:text-slate-700 transition-colors"
              >
                {open
                  ? <ChevronDown className="w-4 h-4" aria-hidden="true" />
                  : <ChevronRight className="w-4 h-4" aria-hidden="true" />}
              </button>
            ) : (
              <span className="w-4 h-4 shrink-0" aria-hidden="true" />
            )}
            <span
              className="w-2.5 h-2.5 rounded-full shrink-0"
              style={{ backgroundColor: colorOf(row.channel) }}
              aria-hidden="true"
            />
            {name}
          </span>
        </Td>
        <Td align="right" tabular hideBelow="sm">{formatNumber(row.orders)}</Td>
        <Td align="right" tabular>{formatCurrency(row.revenue)}</Td>
        <Td hideBelow="sm"><ShareBar pct={share} color={colorOf(row.channel)} /></Td>
        <Td align="right" tabular hideBelow="lg">
          {row.orders > 0 ? formatCurrency(row.revenue / row.orders) : '—'}
        </Td>
        <Td align="right" tabular hideBelow="md">{formatNumber(row.new_customers)}</Td>
        <Td align="right" tabular hideBelow="sm">
          <ChangeValue change={changeOf(row.revenue, row.previous.revenue)} />
        </Td>
      </Tr>
      {open && row.details.map((d) => (
        <Tr key={`${row.channel}:${d.label}`} hover={false}>
          <Td>
            <span
              className="block pl-6 sm:pl-10 text-xs text-slate-600 truncate max-w-[10rem] sm:max-w-md"
              title={d.label || undefined}
            >
              {d.label || t('traffic.organicNoTag')}
            </span>
          </Td>
          <Td align="right" tabular hideBelow="sm">
            <span className="text-xs text-slate-600">{formatNumber(d.orders)}</span>
          </Td>
          <Td align="right" tabular>
            <span className="text-xs text-slate-600">{formatCurrency(d.revenue)}</span>
          </Td>
          <Td hideBelow="sm">
            <ShareBar
              pct={row.revenue > 0 ? (d.revenue / row.revenue) * 100 : 0}
              color={colorOf(row.channel)}
            />
          </Td>
          <Td hideBelow="lg" />
          <Td hideBelow="md" />
          <Td hideBelow="sm" />
        </Tr>
      ))}
      {open && more > 0 && (
        <Tr hover={false}>
          <Td colSpan={7}>
            <span className="block pl-6 sm:pl-10 text-xs text-slate-400">
              {t('traffic.organicMoreDetails', { value: formatNumber(more) })}
            </span>
          </Td>
        </Tr>
      )}
    </Fragment>
  )
})

// ─── Component ────────────────────────────────────────────────────────────────

export const OrganicBreakdown = memo(function OrganicBreakdown() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useTrafficOrganic()
  // Same filters, so its total is the denominator of "share of all revenue" —
  // and it is already in the cache for the cards at the top of the tab.
  const { data: analytics } = useTrafficAnalytics()
  const [open, setOpen] = useState<ReadonlySet<string>>(() => new Set())

  const onToggle = useCallback((channel: string) => {
    setOpen((prev) => {
      const next = new Set(prev)
      if (next.has(channel)) next.delete(channel)
      else next.add(channel)
      return next
    })
  }, [])

  const totals = data?.totals
  const shareOfAll = useMemo(() => {
    const all = analytics?.totals.revenue ?? 0
    return totals && all > 0 ? (totals.revenue / all) * 100 : null
  }, [analytics, totals])

  const channels = data?.channels ?? []
  const isEmpty = !isLoading && channels.length === 0

  return (
    <ChartContainer
      title={t('traffic.organicBreakdown')}
      titleExtra={
        <InfoPopover title={t('traffic.organicHowTitle')} size="wide">
          <Wrapper dir="column" gap="sm">
            <p className="text-xs text-slate-300 leading-relaxed">{t('traffic.organicHowIntro')}</p>
            <ul className="text-xs text-slate-300 leading-relaxed">
              {CHANNELS.map((c) => (
                <li key={c} className="flex items-start gap-2 py-0.5">
                  <span
                    className="mt-1 w-2 h-2 rounded-full shrink-0"
                    style={{ backgroundColor: colorOf(c) }}
                    aria-hidden="true"
                  />
                  <span>
                    <span className="font-medium text-slate-100">{t(`traffic.organicChannel.${c}`)}</span>
                    {' — '}
                    {t(`traffic.organicChannelDesc.${c}`)}
                  </span>
                </li>
              ))}
            </ul>
          </Wrapper>
        </InfoPopover>
      }
      isLoading={isLoading}
      error={error as Error | null}
      onRetry={refetch}
      height="auto"
      isEmpty={isEmpty}
      emptyMessage={t('traffic.organicEmpty')}
      ariaLabel={t('traffic.organicBreakdownDesc')}
    >
      {data && totals && (
        <Wrapper dir="column" gap="md">
          <Wrapper dir="row" gap="md" wrap align="baseline">
            <span className="text-lg font-semibold text-slate-800 tabular-nums">
              {t('traffic.organicTotal')}: {formatCurrency(totals.revenue)}
            </span>
            {shareOfAll !== null && (
              <span className="text-sm text-slate-600">
                {t('traffic.organicShareOfAll', { value: formatPercent(shareOfAll) })}
              </span>
            )}
            <span className="text-sm text-slate-600 tabular-nums">
              {t('traffic.organicOrdersCount', { value: formatNumber(totals.orders) })}
            </span>
            <span className="text-sm text-slate-600 tabular-nums">
              {t('traffic.newCustomersCount', { value: formatNumber(totals.new_customers) })}
            </span>
            <span className="text-sm text-slate-600">
              {t('traffic.organicVsPrevious', {
                value: changeText(changeOf(totals.revenue, data.previous_totals.revenue), t),
              })}
            </span>
          </Wrapper>

          <DataTable variant="feature">
            <thead>
              <Tr header>
                <Th>{t('traffic.organicColChannel')}</Th>
                <Th align="right" hideBelow="sm">{t('traffic.organicColOrders')}</Th>
                <Th align="right">{t('traffic.organicColRevenue')}</Th>
                <Th hideBelow="sm">{t('traffic.organicColShare')}</Th>
                <Th align="right" hideBelow="lg">{t('traffic.organicColAvgOrder')}</Th>
                <Th align="right" hideBelow="md">{t('traffic.organicColNewCustomers')}</Th>
                <Th align="right" hideBelow="sm">{t('traffic.organicColChange')}</Th>
              </Tr>
            </thead>
            <tbody>
              {channels.map((row) => (
                <ChannelRows
                  key={row.channel}
                  row={row}
                  organicRevenue={totals.revenue}
                  open={open.has(row.channel)}
                  onToggle={onToggle}
                />
              ))}
            </tbody>
          </DataTable>

          <p className="text-xs text-slate-400">
            {t('traffic.organicPreviousPeriod', {
              start: formatDate(data.previous_period.start),
              end: formatDate(data.previous_period.end),
            })}
          </p>
        </Wrapper>
      )}
    </ChartContainer>
  )
})

export default OrganicBreakdown
