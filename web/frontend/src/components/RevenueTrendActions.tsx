import type { RevenueForecast } from '../types/api'
import { formatShortCurrency, getCompareTypeOptions } from './revenueTrendHelpers'
import type { CompareType } from './revenueTrendTypes'

interface GrowthTotals {
  current: number
  previous: number
  growth_percent: number
}

interface ActionsProps {
  forecast: RevenueForecast | undefined
  growthData: GrowthTotals | undefined
  isLoading: boolean
  compareType: CompareType
  onCompareTypeChange: (type: CompareType) => void
  t: (key: string) => string
}

export function RevenueTrendActions({
  forecast,
  growthData,
  isLoading,
  compareType,
  onCompareTypeChange,
  t,
}: ActionsProps) {
  return (
    <div className="flex items-center gap-1.5 sm:gap-3 flex-wrap justify-end">
      {forecast && !isLoading && (
        <div className="px-1.5 sm:px-2.5 py-1 rounded-full text-[10px] sm:text-xs font-semibold flex items-center gap-1 bg-blue-50 text-blue-600 border border-blue-200">
          <span>{t('chart.predictedColon')}</span>
          <span>{formatShortCurrency(forecast.predicted_total)}</span>
        </div>
      )}

      {growthData && !isLoading && (
        <div
          className={`px-1.5 sm:px-2.5 py-1 rounded-full text-[10px] sm:text-xs font-semibold flex items-center gap-1 ${
            growthData.growth_percent >= 0
              ? 'bg-emerald-100 text-emerald-700'
              : 'bg-red-100 text-red-700'
          }`}
        >
          <span>{growthData.growth_percent >= 0 ? '↑' : '↓'}</span>
          <span>{Math.abs(growthData.growth_percent).toFixed(1)}%</span>
        </div>
      )}

      <select
        value={compareType}
        onChange={(e) => onCompareTypeChange(e.target.value as CompareType)}
        className="text-[10px] sm:text-xs bg-slate-100 border-0 rounded-lg px-1.5 sm:px-2.5 py-1.5 text-slate-600 font-medium cursor-pointer hover:bg-slate-200 transition-colors focus:ring-2 focus:ring-blue-500 focus:outline-none"
      >
        {getCompareTypeOptions(t).map((opt) => (
          <option key={opt.value} value={opt.value}>
            {t('chart.vs')} {opt.shortLabel}
          </option>
        ))}
      </select>
    </div>
  )
}
