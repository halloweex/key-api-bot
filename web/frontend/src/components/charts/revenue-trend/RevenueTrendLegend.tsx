import { CHART_THEME } from '../config'
import { FORECAST_BAR_COLOR, PREV_MONTH_BAR_COLOR, type PeriodLabels } from './types'

interface LegendProps {
  periodLabels: PeriodLabels
  hasComparison: boolean
  hasPrevMonthDays: boolean
  hasForecast: boolean
  t: (key: string) => string
}

export function RevenueTrendLegend({
  periodLabels,
  hasComparison,
  hasPrevMonthDays,
  hasForecast,
  t,
}: LegendProps) {
  const localeMap: Record<string, string> = { en: 'en-US', uk: 'uk-UA', ru: 'ru-RU' }
  const lang = (typeof window !== 'undefined' && localStorage.getItem('ks_language')) || 'en'
  const currentMonthName = new Date().toLocaleString(localeMap[lang] || 'en-US', { month: 'short' })

  return (
    <div
      style={{
        display: 'flex',
        justifyContent: 'center',
        flexWrap: 'wrap',
        gap: '16px',
        paddingTop: '8px',
        fontSize: '12px',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
        <div
          style={{
            width: '14px',
            height: '14px',
            borderRadius: '3px',
            background: CHART_THEME.primary,
            flexShrink: 0,
          }}
        />
        <span style={{ color: CHART_THEME.text, fontWeight: 500, whiteSpace: 'nowrap' }}>
          {hasPrevMonthDays ? `${currentMonthName} ${t('chart.currentMonthTag')}` : periodLabels.current}
        </span>
      </div>
      {hasPrevMonthDays && (
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <div
            style={{
              width: '14px',
              height: '14px',
              borderRadius: '3px',
              background: PREV_MONTH_BAR_COLOR,
              flexShrink: 0,
            }}
          />
          <span style={{ color: CHART_THEME.muted, whiteSpace: 'nowrap' }}>{t('chart.previousMonth')}</span>
        </div>
      )}
      {hasForecast && (
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <div
            style={{
              width: '14px',
              height: '14px',
              borderRadius: '3px',
              background: FORECAST_BAR_COLOR,
              opacity: 0.7,
              flexShrink: 0,
            }}
          />
          <span style={{ color: CHART_THEME.muted, whiteSpace: 'nowrap' }}>{t('chart.predicted')}</span>
        </div>
      )}
      {hasComparison && (
        <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
          <div
            style={{
              width: '16px',
              height: '0px',
              borderTop: `2px dashed ${CHART_THEME.muted}`,
              flexShrink: 0,
            }}
          />
          <span style={{ color: CHART_THEME.muted, whiteSpace: 'nowrap' }}>{periodLabels.previous}</span>
        </div>
      )}
    </div>
  )
}
