import type {
  RevenueForecast,
  RevenueForecastBlackFriday,
  RevenueForecastRange,
} from '../types/api'
import {
  formatDayRange,
  formatShortCurrency,
  formatShortRange,
  getCompareTypeOptions,
} from './revenueTrendHelpers'
import type { CompareType } from './revenueTrendTypes'
import { Select } from './Select'

interface GrowthTotals {
  current: number
  previous: number
  growth_percent: number
}

interface ActionsProps {
  forecast: RevenueForecast | undefined
  /** The month's 80% range (`/api/revenue/forecast`): pass it for the month only. */
  monthRange?: RevenueForecastRange | null
  /** What last year's Black Friday would add: pass it for the month only. */
  blackFriday?: RevenueForecastBlackFriday | null
  growthData: GrowthTotals | undefined
  isLoading: boolean
  compareType: CompareType
  /** False when the period has nothing before it: no comparison to pick. */
  canCompare?: boolean
  onCompareTypeChange: (type: CompareType) => void
  t: (key: string, options?: Record<string, string | number>) => string
}

export function RevenueTrendActions({
  forecast,
  monthRange,
  blackFriday,
  growthData,
  isLoading,
  compareType,
  canCompare = true,
  onCompareTypeChange,
  t,
}: ActionsProps) {
  const point = forecast?.predicted_total
  // The range is the morning's (data through yesterday) and the point counts
  // today's sales as they come: once a big day has carried the point past an
  // end, the range is not said beside it.
  const range =
    point !== undefined && monthRange && monthRange.lo <= point && point <= monthRange.hi
      ? monthRange
      : null
  const promo = point !== undefined && blackFriday ? blackFriday : null
  const isBlackFriday = promo?.last_year_start.slice(5, 7) === '11'

  // The line about Black Friday sits under the row, not in it: a row that
  // wraps counts every item's width in its own, and would squeeze the title.
  return (
    <div className="flex flex-col items-end gap-1">
      <div className="flex items-center gap-1.5 sm:gap-3 flex-wrap justify-end">
        {forecast && !isLoading && (
          <div className="px-1.5 sm:px-2.5 py-1 rounded-full text-[10px] sm:text-xs font-semibold flex items-center gap-1 bg-blue-50 text-blue-600 border border-blue-200">
            <span>{t('chart.predictedColon')}</span>
            <span>{formatShortCurrency(forecast.predicted_total)}</span>
          </div>
        )}

        {range && !isLoading && (
          <span
            className="text-[10px] sm:text-xs font-medium text-blue-600 whitespace-nowrap"
            title={t('chart.forecastRangeHint')}
          >
            {t('chart.forecastRange', { range: formatShortRange(range.lo, range.hi) })}
          </span>
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

        {canCompare && (
          <Select
            variant="pill"
            options={getCompareTypeOptions(t).map((opt) => ({
              value: opt.value,
              label: `${t('chart.vs')} ${opt.shortLabel}`,
            }))}
            value={compareType}
            onChange={(v) => v && onCompareTypeChange(v as CompareType)}
            allowEmpty={false}
          />
        )}
      </div>

      {promo && point !== undefined && !isLoading && (
        <p
          className="text-right text-[10px] sm:text-xs text-slate-500"
          title={t('chart.blackFridayHint', {
            dates: formatDayRange(promo.last_year_start, promo.last_year_end),
            days: promo.size_days.toFixed(1),
            normal: formatShortCurrency(promo.normal_day),
          })}
        >
          {t(isBlackFriday ? 'chart.blackFridayLine' : 'chart.bigPromoLine', {
            extra: formatShortCurrency(promo.extra),
            total: formatShortCurrency(point + promo.extra),
          })}
        </p>
      )}
    </div>
  )
}
