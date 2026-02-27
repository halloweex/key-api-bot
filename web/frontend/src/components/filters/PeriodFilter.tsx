import { useState, useCallback } from 'react'
import { Calendar } from 'lucide-react'
import { useTranslation } from 'react-i18next'
import { Button } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import { DateRangePicker } from './DateRangePicker'
import type { Period } from '../../types/filters'
import { format } from 'date-fns'

const PERIODS: { value: Period; labelKey: string }[] = [
  { value: 'today', labelKey: 'filter.today' },
  { value: 'yesterday', labelKey: 'filter.yesterday' },
  { value: 'last_7_days', labelKey: 'filter.7days' },
  { value: 'last_28_days', labelKey: 'filter.28days' },
  { value: 'week', labelKey: 'filter.thisWeek' },
  { value: 'last_week', labelKey: 'filter.lastWeek' },
  { value: 'month', labelKey: 'filter.thisMonth' },
  { value: 'last_month', labelKey: 'filter.lastMonth' },
]

export function PeriodFilter() {
  const { t } = useTranslation()
  const { period, startDate, endDate, setPeriod } = useFilterStore()
  const [showDatePicker, setShowDatePicker] = useState(false)

  const handlePeriodChange = useCallback((newPeriod: Period) => {
    setPeriod(newPeriod)
    setShowDatePicker(false)
  }, [setPeriod])

  const handleCustomClick = useCallback(() => {
    setShowDatePicker((prev) => !prev)
  }, [])

  const handleDatePickerClose = useCallback(() => {
    setShowDatePicker(false)
  }, [])

  // Format custom date range for button label
  const customLabel = period === 'custom' && startDate && endDate
    ? startDate === endDate
      ? format(new Date(startDate), 'MMM d')
      : `${format(new Date(startDate), 'MMM d')} - ${format(new Date(endDate), 'MMM d')}`
    : t('filter.custom')

  return (
    <div className="relative flex flex-col sm:flex-row items-stretch sm:items-center gap-2 sm:gap-3">
      <div className="flex items-center gap-0.5 bg-slate-100/80 rounded-lg sm:rounded-xl p-0.5 sm:p-1 border border-slate-200/60 flex-shrink-0 overflow-x-auto scrollbar-hide">
        {PERIODS.map(({ value, labelKey }) => (
          <Button
            key={value}
            size="sm"
            variant={period === value ? 'primary' : 'ghost'}
            onClick={() => handlePeriodChange(value)}
            className={`${period === value ? 'shadow-sm' : ''} whitespace-nowrap text-[10px] sm:text-xs md:text-sm px-1.5 sm:px-2 md:px-3 py-1 sm:py-1.5`}
          >
            {t(labelKey)}
          </Button>
        ))}
        <Button
          size="sm"
          variant={period === 'custom' ? 'primary' : 'ghost'}
          onClick={handleCustomClick}
          className={`${period === 'custom' ? 'shadow-sm' : ''} whitespace-nowrap text-[10px] sm:text-xs md:text-sm px-1.5 sm:px-2 md:px-3 py-1 sm:py-1.5 flex items-center gap-1`}
        >
          <Calendar className="w-3 h-3 sm:w-4 sm:h-4" />
          <span className="hidden sm:inline">{customLabel}</span>
          <span className="sm:hidden">{period === 'custom' ? customLabel : t('filter.custom')}</span>
        </Button>
      </div>
      {showDatePicker && (
        <DateRangePicker onClose={handleDatePickerClose} />
      )}
    </div>
  )
}
