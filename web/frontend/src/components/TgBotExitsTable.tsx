import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from './ChartContainer'
import { DataTable, Tr, Th, Td } from './DataTable'
import { formatNumber } from '../utils/formatters'
import type { TgBotTapRow } from '../types/api'

// After which press people stop: per button, how many visits ended on it and
// how many people pressed it last and then went quiet. Rows come ordered by
// the second, which is the question the page is asked.

interface TgBotExitsTableProps {
  rows: TgBotTapRow[]
  tapsSince: string | null
  silenceDays: number
  sessionGapMinutes: number
  leftTotal: number
  isLoading?: boolean
  error?: Error | null
  onRetry?: () => void
}

export const TgBotExitsTable = memo(function TgBotExitsTable({
  rows, tapsSince, silenceDays, sessionGapMinutes, leftTotal, isLoading, error, onRetry,
}: TgBotExitsTableProps) {
  const { t } = useTranslation()

  return (
    <ChartContainer
      title={t('tgBot.exitsTitle')}
      isLoading={isLoading}
      error={error ?? null}
      onRetry={onRetry}
      isEmpty={!isLoading && rows.length === 0}
      emptyMessage={t('tgBot.exitsEmpty')}
      height="auto"
      ariaLabel={t('tgBot.exitsTitle')}
    >
      <p className="mb-3 text-xs text-slate-500">
        {t('tgBot.exitsHint', { gap: sessionGapMinutes, days: silenceDays, left: leftTotal })}
        {tapsSince && <> {t('tgBot.exitsSince', { date: tapsSince })}</>}
      </p>
      <DataTable variant="feature">
        <thead>
          <Tr header>
            <Th>{t('tgBot.colButton')}</Th>
            <Th align="right" hideBelow="sm">{t('tgBot.colPresses')}</Th>
            <Th align="right" hideBelow="sm">{t('tgBot.colPeople')}</Th>
            <Th align="right">{t('tgBot.colVisitEnded')}</Th>
            <Th align="right">{t('tgBot.colLeftAfter')}</Th>
          </Tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <Tr key={row.what}>
              <Td>
                <span className="text-slate-800">{row.label}</span>
                <span className="block text-[11px] text-slate-400">{row.what}</span>
              </Td>
              <Td align="right" hideBelow="sm" tabular>{formatNumber(row.presses)}</Td>
              <Td align="right" hideBelow="sm" tabular>{formatNumber(row.people)}</Td>
              <Td align="right" tabular>{formatNumber(row.session_exits)}</Td>
              <Td align="right" tabular bold={row.left_after > 0}>{formatNumber(row.left_after)}</Td>
            </Tr>
          ))}
        </tbody>
      </DataTable>
    </ChartContainer>
  )
})

export default TgBotExitsTable
