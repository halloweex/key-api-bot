import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { PageShell } from './PageShell'
import { PageHeading } from './PageHeading'
import { ChartGrid } from './ChartGrid'
import { TileGrid } from './TileGrid'
import { MetricCard } from './MetricCard'
import { InfoBanner } from './InfoBanner'
import { SegmentedControl } from './SegmentedControl'
import { TgBotActivityChart } from './TgBotActivityChart'
import { TgBotWeeklyChart } from './TgBotWeeklyChart'
import { useTgBotActivity } from '../hooks/useApi'
import { formatNumber, formatPercent } from '../utils/formatters'

// The customer Telegram bot's activity. Admin-only. Every number is the bot's
// own readout, computed by the bot (Kyiv days, admins excluded) and printed by
// its /stats from the same function — this page only draws it.

type Window = '30' | '90' | '180'

export const TgBotPage = memo(function TgBotPage() {
  const { t } = useTranslation()
  const [span, setSpan] = useState<Window>('90')
  const { data, isLoading, error, refetch } = useTgBotActivity(Number(span))
  const failure = (error as Error | null) ?? null
  const dash = '—'

  return (
    <PageShell variant="feature">
      <PageHeading
        title={t('nav.tgBot')}
        subtitle={t('tgBot.subtitle')}
        actions={
          <SegmentedControl<Window>
            ariaLabel={t('tgBot.window')}
            size="sm"
            value={span}
            onChange={setSpan}
            options={(['30', '90', '180'] as const).map((v) => ({
              value: v, label: t('tgBot.days', { count: Number(v) }),
            }))}
          />
        }
      />

      {data?.stale && (
        <InfoBanner title={t('tgBot.staleTitle')}>
          {t('tgBot.staleBody', { time: data.fetched_at.replace('T', ' ').replace('Z', ' UTC') })}
        </InfoBanner>
      )}

      {data && (
        <TileGrid columns={3}>
          <MetricCard
            label={t('tgBot.users')}
            value={formatNumber(data.users_total)}
            sub={t('tgBot.registeredOf', { count: data.registered_total })}
          />
          <MetricCard
            label={t('tgBot.registered7d')}
            value={formatNumber(data.registered_7d)}
            sub={t('tgBot.previous7', { count: data.registered_prev_7d })}
          />
          <MetricCard
            label={t('tgBot.activeDays', { count: data.wau_days })}
            value={formatNumber(data.wau)}
          />
          <MetricCard
            label={t('tgBot.activeDays', { count: data.mau_days })}
            value={formatNumber(data.mau)}
          />
          <MetricCard
            label={t('tgBot.stickiness')}
            value={data.stickiness === null ? dash : formatPercent(data.stickiness, 0)}
            sub={data.stickiness === null ? t('tgBot.tooFew') : t('tgBot.stickinessHint')}
          />
          <MetricCard
            label={t('tgBot.activation')}
            value={data.activation === null ? dash : formatPercent(data.activation, 0)}
            sub={t('tgBot.activationHint', {
              days: data.activation_days,
              registered: data.activation_registered,
              cohort: data.activation_cohort,
            })}
          />
        </TileGrid>
      )}

      <ChartGrid>
        <TgBotActivityChart
          daily={data?.daily ?? []}
          logStarts={data?.log_starts ?? null}
          isLoading={isLoading}
          error={failure}
          onRetry={refetch}
        />
        <TgBotWeeklyChart
          weekly={data?.weekly ?? []}
          isLoading={isLoading}
          error={failure}
          onRetry={refetch}
        />
      </ChartGrid>
    </PageShell>
  )
})

export default TgBotPage
