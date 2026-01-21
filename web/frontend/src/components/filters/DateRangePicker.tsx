import { useState, useCallback } from 'react'
import { Button } from '../ui'
import { useFilterStore } from '../../store/filterStore'

interface DateRangePickerProps {
  onClose?: () => void
}

export function DateRangePicker({ onClose }: DateRangePickerProps) {
  const { startDate, endDate, setCustomDates } = useFilterStore()

  // Initialize with current values or today
  const today = new Date().toISOString().split('T')[0]
  const [start, setStart] = useState(startDate || today)
  const [end, setEnd] = useState(endDate || today)

  const handleApply = useCallback(() => {
    if (start && end) {
      setCustomDates(start, end)
      onClose?.()
    }
  }, [start, end, setCustomDates, onClose])

  const isValid = start && end && start <= end

  return (
    <div className="flex flex-col sm:flex-row items-stretch sm:items-center gap-2">
      <div className="flex items-center gap-2">
        <input
          type="date"
          value={start}
          onChange={(e) => setStart(e.target.value)}
          className="flex-1 min-w-0 px-2 sm:px-3 py-1.5 text-xs sm:text-sm bg-white border border-slate-300 rounded-lg text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        <span className="text-slate-600 text-xs sm:text-sm">to</span>
        <input
          type="date"
          value={end}
          onChange={(e) => setEnd(e.target.value)}
          max={today}
          className="flex-1 min-w-0 px-2 sm:px-3 py-1.5 text-xs sm:text-sm bg-white border border-slate-300 rounded-lg text-slate-900 focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
      </div>
      <div className="flex items-center gap-2">
        <Button
          size="sm"
          variant="primary"
          onClick={handleApply}
          disabled={!isValid}
          className="flex-1 sm:flex-none"
        >
          Apply
        </Button>
        {onClose && (
          <Button
            size="sm"
            variant="ghost"
            onClick={onClose}
            className="flex-1 sm:flex-none"
          >
            Cancel
          </Button>
        )}
      </div>
    </div>
  )
}
