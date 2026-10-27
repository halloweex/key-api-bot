import { Fragment, memo, useCallback, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { ChartContainer } from './ChartContainer'
import { DataTable, Tr, Th, Td } from './DataTable'
import { InfoPopover } from './InfoPopover'
import { Wrapper } from './Wrapper'
import { useTrafficAnalytics, useTrafficOrganic } from '../hooks/useApi'
import { formatCurrency, formatDate, formatNumber, formatPercent } from '../utils/formatters'
import type { OrganicChannel, OrganicChannelDetail, OrganicChannelKey } from '../types/api'

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

// The group each channel sits in, named in GA4's own vocabulary — the words a
// marketer already reads in "Traffic acquisition". `null` is a sale with no
// visit to the website at all, a manager's order in Direct or Telegram: GA4
// never sees it, and saying so is half of what makes Direct legible. Telegram
// holds both kinds. A name, not a promise: GA4 files raw tags by its own rules
// and puts some elsewhere (`klaviyo / klaviyo` lands in Unassigned there),
// which the explanation says in so many words.
const GA4_GROUPS: Record<OrganicChannelKey, Array<string | null>> = {
  instagram_direct: [null],
  instagram_site: ['Organic Social'],
  instagram_stories: ['Organic Social'],
  telegram: [null, 'Organic Social'],
  email_campaigns: ['Email'],
  email_flows: ['Email'],
  google_organic: ['Organic Shopping'],
  ai: ['Referral'],
  facebook: ['Organic Social'],
  tiktok: ['Organic Social'],
  other: ['Referral'],
}

function ga4Group(channel: string, t: (key: string) => string): string {
  const groups = GA4_GROUPS[channel as OrganicChannelKey] ?? GA4_GROUPS.other
  return groups.map((g) => g ?? t('traffic.organicGa4Offsite')).join(' / ')
}

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

// ─── One source / medium row ─────────────────────────────────────────────────
//
// GA4's traffic acquisition report: "source / medium" first, the campaign and
// content beneath it. An order with no tags at all is named, not hidden — for
// Direct that row is the whole answer to what the channel is made of.

const DetailLabel = memo(function DetailLabel({ detail }: { detail: OrganicChannelDetail }) {
  const { t } = useTranslation()
  const untagged = !detail.source && !detail.medium && !detail.campaign && !detail.content
  const notSet = t('traffic.organicNotSet')
  const main = untagged
    ? t('traffic.organicNoUtm')
    : `${detail.source || notSet} / ${detail.medium || notSet}`
  const sub = untagged
    ? t('traffic.organicNoUtmHint')
    : [detail.campaign, detail.content].filter(Boolean).join(' · ')

  return (
    <span className="block pl-6 sm:pl-10 max-w-[11rem] sm:max-w-md">
      <span className="block text-xs text-slate-700 truncate" title={main}>{main}</span>
      {sub && (
        <span className="block text-[11px] text-slate-400 truncate" title={sub}>{sub}</span>
      )}
    </span>
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
            <span className="flex flex-col">
              <span>{name}</span>
              <span className="text-xs font-normal text-slate-400">
                {t('traffic.organicGa4Group', { value: ga4Group(row.channel, t) })}
              </span>
            </span>
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
      {open && row.details.length > 0 && (
        <Tr hover={false}>
          <Td colSpan={7}>
            <span className="block pl-6 sm:pl-10 text-[11px] uppercase tracking-wide text-slate-400">
              {t('traffic.organicDetailHeader')}
            </span>
          </Td>
        </Tr>
      )}
      {open && row.details.map((d) => (
        <Tr
          key={`${row.channel}:${d.source}/${d.medium}/${d.campaign}/${d.content}`}
          hover={false}
        >
          <Td>
            <DetailLabel detail={d} />
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
            <table className="w-full text-xs text-slate-300 leading-snug">
              <thead>
                <tr className="text-left text-slate-400">
                  <th className="py-1 pr-2 font-medium">{t('traffic.organicColChannel')}</th>
                  <th className="py-1 pr-2 font-medium">{t('traffic.organicHowColWhat')}</th>
                  <th className="py-1 font-medium">{t('traffic.organicHowColGa4')}</th>
                </tr>
              </thead>
              <tbody>
                {CHANNELS.map((c) => (
                  <tr key={c} className="border-t border-slate-700 align-top">
                    <td className="py-1.5 pr-2">
                      <span className="inline-flex items-start gap-1.5">
                        <span
                          className="mt-1 w-2 h-2 rounded-full shrink-0"
                          style={{ backgroundColor: colorOf(c) }}
                          aria-hidden="true"
                        />
                        <span className="font-medium text-slate-100">
                          {t(`traffic.organicChannel.${c}`)}
                        </span>
                      </span>
                    </td>
                    <td className="py-1.5 pr-2">{t(`traffic.organicChannelDesc.${c}`)}</td>
                    <td className="py-1.5 text-slate-200">{ga4Group(c, t)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
            <p className="text-[11px] text-slate-400 leading-relaxed">{t('traffic.organicGa4Note')}</p>
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
