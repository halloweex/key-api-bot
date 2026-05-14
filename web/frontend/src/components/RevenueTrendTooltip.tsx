import { CHART_THEME, TOOLTIP_STYLE } from './chartConfig'
import { formatCurrency } from '../utils/formatters'
import { FORECAST_BAR_COLOR, type ChartDataPoint, type PeriodLabels } from './revenueTrendTypes'

interface TooltipProps {
  active?: boolean
  payload?: Array<{
    value: number
    dataKey: string
    payload: ChartDataPoint
  }>
  label?: string
  periodLabels: PeriodLabels
  t: (key: string) => string
}

export function RevenueTrendTooltip({ active, payload, periodLabels, t }: TooltipProps) {
  if (!active || !payload?.length) return null

  const data = payload[0]?.payload
  if (!data) return null

  const isPartialDay = data.revenue > 0 && (data.fullDayForecast ?? 0) > 0
  const displayRevenue = data.isForecast ? data.forecastRevenue : data.revenue
  const hasComparison = data.prevRevenue > 0
  const isPositive = data.change >= 0
  const changeColor = isPositive ? CHART_THEME.primary : CHART_THEME.danger
  const changeIcon = isPositive ? '↑' : '↓'

  return (
    <div
      style={{
        ...TOOLTIP_STYLE,
        padding: '10px 12px',
        minWidth: '180px',
        maxWidth: '280px',
      }}
    >
      <p style={{ fontWeight: 600, marginBottom: '10px', color: CHART_THEME.text, fontSize: '13px' }}>
        {data.date}
        {data.isForecast && (
          <span style={{ color: CHART_THEME.muted, fontWeight: 400, fontSize: '11px', marginLeft: '6px' }}>
            {t('chart.predictedLabel')}
          </span>
        )}
        {isPartialDay && (
          <span style={{ color: CHART_THEME.muted, fontWeight: 400, fontSize: '11px', marginLeft: '6px' }}>
            {t('chart.todayLabel')}
          </span>
        )}
      </p>

      {/* Current Period — actual revenue */}
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: '6px',
          paddingBottom: hasComparison || isPartialDay ? '6px' : '0',
          borderBottom: hasComparison || isPartialDay ? `1px solid ${CHART_THEME.border}` : 'none',
        }}
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <div
            style={{
              width: '12px',
              height: '12px',
              borderRadius: '3px',
              background: data.isForecast ? FORECAST_BAR_COLOR : CHART_THEME.primary,
            }}
          />
          <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>
            {data.isForecast ? t('chart.predicted') : isPartialDay ? t('chart.actualSoFar') : periodLabels.current}
          </span>
        </div>
        <span style={{ fontWeight: 600, color: data.isForecast ? FORECAST_BAR_COLOR : CHART_THEME.primary }}>
          {formatCurrency(displayRevenue)}
        </span>
      </div>

      {/* Full-day prediction for today (partial day) */}
      {isPartialDay && (
        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            marginBottom: '6px',
            paddingBottom: hasComparison ? '6px' : '0',
            borderBottom: hasComparison ? `1px solid ${CHART_THEME.border}` : 'none',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <div
              style={{
                width: '12px',
                height: '12px',
                borderRadius: '3px',
                background: FORECAST_BAR_COLOR,
              }}
            />
            <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>
              {t('chart.predictedTotal')}
            </span>
          </div>
          <span style={{ fontWeight: 600, color: FORECAST_BAR_COLOR }}>
            {formatCurrency(data.fullDayForecast!)}
          </span>
        </div>
      )}

      {/* Previous Period */}
      {hasComparison && data.prevRevenue > 0 && (
        <>
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: '8px',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <div
                style={{
                  width: '14px',
                  height: '0px',
                  borderTop: `2px dashed ${CHART_THEME.muted}`,
                }}
              />
              <span style={{ color: CHART_THEME.muted, fontSize: '12px' }}>
                {periodLabels.previous}
              </span>
            </div>
            <span style={{ fontWeight: 500, color: CHART_THEME.muted }}>
              {formatCurrency(data.prevRevenue)}
            </span>
          </div>

          {/* Change Indicator */}
          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              background: isPositive ? 'rgba(37, 99, 235, 0.1)' : 'rgba(239, 68, 68, 0.1)',
              padding: '6px 8px',
              borderRadius: '6px',
              marginTop: '4px',
            }}
          >
            <span style={{ color: CHART_THEME.text, fontSize: '12px', fontWeight: 500 }}>
              {t('chart.vsPrevious')}
            </span>
            <span style={{ fontWeight: 700, color: changeColor, fontSize: '13px' }}>
              {changeIcon} {Math.abs(data.changePercent).toFixed(1)}%
            </span>
          </div>
        </>
      )}
    </div>
  )
}
