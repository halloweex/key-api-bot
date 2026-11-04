import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from './ChartContainer'
import { DataTable, Tr, Th, Td } from './DataTable'
import { InfoPopover } from './InfoPopover'
import { SegmentedControl } from './SegmentedControl'
import { Wrapper } from './Wrapper'
import { useTrafficFunnel } from '../hooks/useApi'
import { ApiError } from '../api/client'
import { formatNumber, formatPercent } from '../utils/formatters'
import type { FunnelCounts, FunnelRow, FunnelStep, TrafficFunnelResponse } from '../types/api'

// ─── The funnel's arithmetic ──────────────────────────────────────────────────
//
// Each rate is the share of people at one step who reached the next. The four
// named ones are what the shop calls them; "overall" is bought out of entered.

const STEPS: FunnelStep[] = ['entered', 'viewed_product', 'added_to_cart', 'began_checkout', 'purchased']

const RATES = [
  { key: 'ppv', from: 'entered', to: 'viewed_product' },
  { key: 'acr', from: 'viewed_product', to: 'added_to_cart' },
  { key: 'car', from: 'added_to_cart', to: 'began_checkout' },
  { key: 'checkout', from: 'began_checkout', to: 'purchased' },
  { key: 'overall', from: 'entered', to: 'purchased' },
] as const

type RateKey = (typeof RATES)[number]['key']
type Breakdown = 'type' | 'channel' | 'device'

const KNOWN_DEVICES = new Set(['mobile', 'desktop', 'tablet'])
const BAR_COLOR = '#3B82F6'

function rate(counts: FunnelCounts | undefined, from: FunnelStep, to: FunnelStep): number | null {
  if (!counts || counts[from] <= 0) return null
  return (counts[to] / counts[from]) * 100
}

function rateOf(counts: FunnelCounts | undefined, key: RateKey): number | null {
  const r = RATES.find((x) => x.key === key)
  return r ? rate(counts, r.from, r.to) : null
}

function hasData(counts: FunnelCounts | undefined): boolean {
  return !!counts && counts.entered > 0
}

// ─── Pieces ───────────────────────────────────────────────────────────────────

/** A change in percentage points, coloured by direction; nothing when unknown. */
const Delta = memo(function Delta({ now, before }: { now: number | null; before: number | null }) {
  const { t } = useTranslation()
  if (now == null || before == null) return null
  const diff = now - before
  const tone = Math.abs(diff) < 0.05 ? 'text-slate-400' : diff > 0 ? 'text-emerald-600' : 'text-rose-600'
  const sign = diff > 0 ? '+' : diff < 0 ? '−' : ''
  return (
    <span className={`tabular-nums ${tone}`}>
      {t('traffic.funnelPp', { value: `${sign}${Math.abs(diff).toFixed(1)}` })}
    </span>
  )
})

const StepRow = memo(function StepRow({
  step, data, showYearAgo,
}: { step: FunnelStep; data: TrafficFunnelResponse; showYearAgo: boolean }) {
  const { t } = useTranslation()
  const now = data.total.current
  const width = now.entered > 0 ? (now[step] / now.entered) * 100 : 0
  return (
    <div className="grid grid-cols-[minmax(0,1fr)_5.5rem] sm:grid-cols-[13rem_minmax(0,1fr)_6rem] gap-x-3 gap-y-1 items-center">
      <span className="text-sm text-slate-700 col-span-2 sm:col-span-1">{t(`traffic.funnelStep.${step}`)}</span>
      <div className="h-6 rounded bg-slate-100 overflow-hidden">
        <div className="h-full rounded" style={{ width: `${Math.max(0.5, width)}%`, backgroundColor: BAR_COLOR }} />
      </div>
      <span className="text-right text-sm font-semibold tabular-nums text-slate-800">
        {formatNumber(now[step])}
        {showYearAgo && hasData(data.total.year_ago) && (
          <span className="block text-xs font-normal text-slate-400">
            {t('traffic.funnelYearAgoCount', { value: formatNumber(data.total.year_ago[step]) })}
          </span>
        )}
      </span>
    </div>
  )
})

const RateRow = memo(function RateRow({
  rateKey, data, showPrevious, showYearAgo,
}: { rateKey: RateKey; data: TrafficFunnelResponse; showPrevious: boolean; showYearAgo: boolean }) {
  const { t } = useTranslation()
  const now = rateOf(data.total.current, rateKey)
  return (
    <div className="flex flex-wrap items-baseline gap-x-3 gap-y-0.5 text-xs text-slate-500 sm:pl-[13.75rem]">
      <span>
        {t(`traffic.funnelRate.${rateKey}`)}:{' '}
        <span className="font-semibold text-slate-700 tabular-nums">{now == null ? '—' : formatPercent(now)}</span>
      </span>
      {showPrevious && (
        <span>{t('traffic.funnelVsPrevious')} <Delta now={now} before={rateOf(data.total.previous, rateKey)} /></span>
      )}
      {showYearAgo && (
        <span>{t('traffic.funnelVsYearAgo')} <Delta now={now} before={rateOf(data.total.year_ago, rateKey)} /></span>
      )}
    </div>
  )
})

const BreakdownTable = memo(function BreakdownTable({
  rows, breakdown, showPrevious,
}: { rows: FunnelRow[]; breakdown: Breakdown; showPrevious: boolean }) {
  const { t } = useTranslation()
  const name = (row: FunnelRow) => {
    if (breakdown === 'type') return t(`traffic.funnelType.${row.key}`)
    if (breakdown === 'device') return KNOWN_DEVICES.has(row.key) ? t(`traffic.funnelDevice.${row.key}`) : row.key
    return row.key
  }
  return (
    <DataTable variant="feature">
      <thead>
        <Tr header>
          <Th>{t(`traffic.funnelBy.${breakdown}`)}</Th>
          <Th align="right">{t('traffic.funnelStep.entered')}</Th>
          {RATES.map((r) => (
            <Th key={r.key} align="right" hideBelow={r.key === 'overall' ? undefined : 'sm'}>
              {t(`traffic.funnelRateShort.${r.key}`)}
            </Th>
          ))}
        </Tr>
      </thead>
      <tbody>
        {rows.filter((row) => hasData(row.current) || hasData(row.previous)).map((row) => (
          <Tr key={row.key}>
            <Td bold>
              <span className="flex flex-col">
                <span>{name(row)}</span>
                {breakdown === 'channel' && row.type && (
                  <span className="text-xs font-normal text-slate-400">{t(`traffic.funnelType.${row.type}`)}</span>
                )}
              </span>
            </Td>
            <Td align="right" tabular>{formatNumber(row.current.entered)}</Td>
            {RATES.map((r) => {
              const now = rateOf(row.current, r.key)
              return (
                <Td key={r.key} align="right" tabular hideBelow={r.key === 'overall' ? undefined : 'sm'}>
                  <span className="flex flex-col items-end">
                    <span>{now == null ? '—' : formatPercent(now)}</span>
                    {showPrevious && (
                      <span className="text-xs"><Delta now={now} before={rateOf(row.previous, r.key)} /></span>
                    )}
                  </span>
                </Td>
              )
            })}
          </Tr>
        ))}
      </tbody>
    </DataTable>
  )
})

// ─── Component ────────────────────────────────────────────────────────────────

export const SiteFunnel = memo(function SiteFunnel() {
  const { t } = useTranslation()
  const { data, isLoading, error, refetch } = useTrafficFunnel()
  const [breakdown, setBreakdown] = useState<Breakdown>('type')

  const notConfigured = error instanceof ApiError && error.status === 503
    && error.message === 'GA4 is not configured'
  const unavailable = error instanceof ApiError && error.status === 503 && !notConfigured

  const showPrevious = !!data && data.periods.previous.start >= data.earliest && hasData(data.total.previous)
  const showYearAgo = !!data && data.periods.year_ago.start >= data.earliest && hasData(data.total.year_ago)
  const rows = !data ? [] : breakdown === 'type' ? data.by_type : breakdown === 'channel' ? data.by_channel : data.by_device

  return (
    <ChartContainer
      title={t('traffic.funnelTitle')}
      titleExtra={
        <InfoPopover title={t('traffic.funnelHowTitle')} size="wide">
          <p className="text-xs text-slate-300 leading-relaxed">{t('traffic.funnelHowText')}</p>
        </InfoPopover>
      }
      isLoading={isLoading}
      error={notConfigured || unavailable ? null : (error as Error | null)}
      onRetry={refetch}
      height="auto"
      isEmpty={notConfigured || unavailable || (!isLoading && !!data && !hasData(data.total.current))}
      emptyMessage={
        notConfigured ? t('traffic.funnelNotConfigured')
          : unavailable ? t('traffic.funnelUnavailable')
            : t('traffic.funnelEmpty')
      }
      ariaLabel={t('traffic.funnelDesc')}
    >
      {data && (
        <Wrapper dir="column" gap="md">
          <Wrapper dir="column" gap="xs">
            {STEPS.map((step, i) => (
              <Wrapper key={step} dir="column" gap="xs">
                <StepRow step={step} data={data} showYearAgo={showYearAgo} />
                {i < RATES.length - 1 && (
                  <RateRow rateKey={RATES[i].key} data={data} showPrevious={showPrevious} showYearAgo={showYearAgo} />
                )}
              </Wrapper>
            ))}
            <RateRow rateKey="overall" data={data} showPrevious={showPrevious} showYearAgo={showYearAgo} />
          </Wrapper>

          {(!showYearAgo || data.stale) && (
            <p className="text-xs text-slate-500">
              {!showYearAgo && t('traffic.funnelNoYearAgo', { date: data.earliest })}
              {!showYearAgo && data.stale && ' '}
              {data.stale && t('traffic.funnelStale')}
            </p>
          )}

          <SegmentedControl<Breakdown>
            options={[
              { value: 'type', label: t('traffic.funnelBy.type') },
              { value: 'channel', label: t('traffic.funnelBy.channel') },
              { value: 'device', label: t('traffic.funnelBy.device') },
            ]}
            value={breakdown}
            onChange={setBreakdown}
            ariaLabel={t('traffic.funnelBreakdownLabel')}
          />
          <BreakdownTable rows={rows} breakdown={breakdown} showPrevious={showPrevious} />
        </Wrapper>
      )}
    </ChartContainer>
  )
})

export default SiteFunnel
