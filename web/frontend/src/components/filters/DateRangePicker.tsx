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
    <div className="flex items-center gap-2">
      <input
        type="date"
        value={start}
        onChange={(e) => setStart(e.target.value)}
        className="px-3 py-1.5 text-sm bg-slate-700 border border-slate-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
      />
      <span className="text-slate-400">to</span>
      <input
        type="date"
        value={end}
        onChange={(e) => setEnd(e.target.value)}
        max={today}
        className="px-3 py-1.5 text-sm bg-slate-700 border border-slate-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
      />
      <Button
        size="sm"
        variant="primary"
        onClick={handleApply}
        disabled={!isValid}
      >
        Apply
      </Button>
      {onClose && (
        <Button
          size="sm"
          variant="ghost"
          onClick={onClose}
        >
          Cancel
        </Button>
      )}
    </div>
  )
}
