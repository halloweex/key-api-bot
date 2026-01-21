import { useState, useCallback } from 'react'
import { Button } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import { DateRangePicker } from './DateRangePicker'
import type { Period } from '../../types/filters'

const PERIODS: { value: Period; label: string; group?: 'fixed' | 'calendar' | 'rolling' }[] = [
  // Fixed periods
  { value: 'today', label: 'Today', group: 'fixed' },
  { value: 'yesterday', label: 'Yesterday', group: 'fixed' },
  // Rolling periods (always have data)
  { value: 'last_7_days', label: '7 Days', group: 'rolling' },
  { value: 'last_28_days', label: '28 Days', group: 'rolling' },
  // Calendar periods
  { value: 'week', label: 'This Week', group: 'calendar' },
  { value: 'last_week', label: 'Last Week', group: 'calendar' },
  { value: 'month', label: 'This Month', group: 'calendar' },
  { value: 'last_month', label: 'Last Month', group: 'calendar' },
]

export function PeriodFilter() {
  const { period, setPeriod } = useFilterStore()
  const [showDatePicker, setShowDatePicker] = useState(false)

  const handlePeriodChange = useCallback((newPeriod: Period) => {
    setPeriod(newPeriod)
    setShowDatePicker(false)
  }, [setPeriod])

  const handleCustomClick = useCallback(() => {
    setShowDatePicker(true)
  }, [])

  const handleDatePickerClose = useCallback(() => {
    setShowDatePicker(false)
  }, [])

  return (
    <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-2 sm:gap-3">
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
          className={`${period === 'custom' ? 'shadow-sm' : ''} whitespace-nowrap text-[10px] sm:text-xs md:text-sm px-1.5 sm:px-2 md:px-3 py-1 sm:py-1.5`}
        >
          Custom
        </Button>
      </div>
      {(showDatePicker || period === 'custom') && (
        <DateRangePicker onClose={handleDatePickerClose} />
      )}
    </div>
  )
}
