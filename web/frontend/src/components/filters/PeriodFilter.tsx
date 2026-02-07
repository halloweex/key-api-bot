import { useState, useCallback } from 'react'
import { Button } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import { DateRangePicker } from './DateRangePicker'
import type { Period } from '../../types/filters'
import { format } from 'date-fns'

const PERIODS: { value: Period; label: string }[] = [
  { value: 'today', label: 'Today' },
  { value: 'yesterday', label: 'Yesterday' },
  { value: 'last_7_days', label: '7 Days' },
  { value: 'last_28_days', label: '28 Days' },
  { value: 'week', label: 'This Week' },
  { value: 'last_week', label: 'Last Week' },
  { value: 'month', label: 'This Month' },
  { value: 'last_month', label: 'Last Month' },
]

export function PeriodFilter() {
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
    : 'Custom'

  return (
    <div className="relative flex flex-col sm:flex-row items-stretch sm:items-center gap-2 sm:gap-3">
      <div className="flex items-center gap-0.5 bg-slate-100/80 rounded-lg sm:rounded-xl p-0.5 sm:p-1 border border-slate-200/60 flex-shrink-0 overflow-x-auto scrollbar-hide">
        {PERIODS.map(({ value, label }) => (
          <Button
            key={value}
            size="sm"
            variant={period === value ? 'primary' : 'ghost'}
            onClick={() => handlePeriodChange(value)}
            className={`${period === value ? 'shadow-sm' : ''} whitespace-nowrap text-[10px] sm:text-xs md:text-sm px-1.5 sm:px-2 md:px-3 py-1 sm:py-1.5`}
          >
            {label}
          </Button>
        ))}
        <Button
          size="sm"
          variant={period === 'custom' ? 'primary' : 'ghost'}
          onClick={handleCustomClick}
          className={`${period === 'custom' ? 'shadow-sm' : ''} whitespace-nowrap text-[10px] sm:text-xs md:text-sm px-1.5 sm:px-2 md:px-3 py-1 sm:py-1.5 flex items-center gap-1`}
        >
          <svg className="w-3 h-3 sm:w-4 sm:h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          <span className="hidden sm:inline">{customLabel}</span>
          <span className="sm:hidden">{period === 'custom' ? customLabel : 'Custom'}</span>
        </Button>
      </div>
      {showDatePicker && (
        <DateRangePicker onClose={handleDatePickerClose} />
      )}
    </div>
  )
}
