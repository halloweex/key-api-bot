import { useState, useCallback } from 'react'
import { Button } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import { DateRangePicker } from './DateRangePicker'
import type { Period } from '../../types/filters'

const PERIODS: { value: Period; label: string }[] = [
  { value: 'today', label: 'Today' },
  { value: 'yesterday', label: 'Yesterday' },
  { value: 'week', label: 'Week' },
  { value: 'last_week', label: 'Last Week' },
  { value: 'month', label: 'Month' },
  { value: 'last_month', label: 'Last Month' },
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
    <div className="flex items-center gap-2 flex-wrap">
      <div className="flex items-center gap-1 bg-slate-700/50 rounded-lg p-1">
        {PERIODS.map(({ value, label }) => (
          <Button
            key={value}
            size="sm"
            variant={period === value ? 'primary' : 'ghost'}
            onClick={() => handlePeriodChange(value)}
          >
            {label}
          </Button>
        ))}
        <Button
          size="sm"
          variant={period === 'custom' ? 'primary' : 'ghost'}
          onClick={handleCustomClick}
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
