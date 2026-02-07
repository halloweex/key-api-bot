import { useState, useCallback } from 'react'
import { Button } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import { DateRangePicker } from './DateRangePicker'
import type { Period } from '../../types/filters'
import { format } from 'date-fns'

const PERIODS: { value: Period; label: string; mobileLabel: string }[] = [
  { value: 'today', label: 'Today', mobileLabel: 'Today' },
  { value: 'yesterday', label: 'Yesterday', mobileLabel: 'Yest.' },
  { value: 'last_7_days', label: '7 Days', mobileLabel: '7d' },
  { value: 'last_28_days', label: '28 Days', mobileLabel: '28d' },
  { value: 'week', label: 'This Week', mobileLabel: 'Week' },
  { value: 'last_week', label: 'Last Week', mobileLabel: 'Prev W' },
  { value: 'month', label: 'This Month', mobileLabel: 'Month' },
  { value: 'last_month', label: 'Last Month', mobileLabel: 'Prev M' },
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
      <div className="flex items-center gap-0.5 bg-slate-100/80 rounded-xl p-1 border border-slate-200/60 overflow-x-auto scrollbar-hide">
        {/* Period buttons */}
        {PERIODS.map(({ value, label, mobileLabel }) => (
          <Button
            key={value}
            size="sm"
            variant={period === value ? 'primary' : 'ghost'}
            onClick={() => handlePeriodChange(value)}
            className={`${period === value ? 'shadow-sm' : ''} whitespace-nowrap text-xs px-2 sm:px-3 py-1.5 min-h-[36px] sm:min-h-[32px] rounded-lg`}
          >
            <span className="sm:hidden">{mobileLabel}</span>
            <span className="hidden sm:inline">{label}</span>
          </Button>
        ))}

        {/* Custom date button */}
        <Button
          size="sm"
          variant={period === 'custom' ? 'primary' : 'ghost'}
          onClick={handleCustomClick}
          className={`${period === 'custom' ? 'shadow-sm' : ''} whitespace-nowrap text-xs px-2 sm:px-3 py-1.5 min-h-[36px] sm:min-h-[32px] rounded-lg flex items-center gap-1`}
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M8 7V3m8 4V3m-9 8h10M5 21h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z" />
          </svg>
          <span className="hidden sm:inline">{customLabel}</span>
          <span className="sm:hidden">{period === 'custom' ? customLabel : ''}</span>
        </Button>
      </div>

      {showDatePicker && (
        <DateRangePicker onClose={handleDatePickerClose} />
      )}
    </div>
  )
}
