import { InfoPopover } from '../../ui/InfoPopover'
import { FORECAST_BAR_COLOR } from './types'

interface InfoProps {
  title: string
  hasForecast: boolean
  t: (key: string) => string
}

export function RevenueTrendInfo({ title, hasForecast, t }: InfoProps) {
  return (
    <InfoPopover title={title}>
      <div className="space-y-2">
        <p className="text-xs text-slate-300">
          <strong className="text-blue-400">{t('chart.rtBarsLabel')}</strong> {t('chart.rtBarsDesc')}
        </p>
        {hasForecast && (
          <p className="text-xs text-slate-300">
            <strong style={{ color: FORECAST_BAR_COLOR }}>{t('chart.rtForecastLabel')}</strong> {t('chart.rtForecastDesc')}
          </p>
        )}
        <p className="text-xs text-slate-300">
          <strong className="text-slate-400">{t('chart.rtDashedLabel')}</strong> {t('chart.rtDashedDesc')}
        </p>
        <p className="text-xs text-slate-300">
          <strong className="text-emerald-400">{t('chart.rtGrowthLabel')}</strong> {t('chart.rtGrowthDesc')}
        </p>
        <p className="text-xs text-slate-300">
          <strong className="text-amber-400">{t('chart.rtPeakLabel')}</strong> {t('chart.rtPeakDesc')}
        </p>
      </div>
    </InfoPopover>
  )
}
