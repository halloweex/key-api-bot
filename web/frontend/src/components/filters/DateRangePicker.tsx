import { useState, useCallback, useMemo } from 'react'
import {
  format,
  startOfMonth,
  endOfMonth,
  startOfWeek,
  endOfWeek,
  addDays,
  addMonths,
  subMonths,
  isSameMonth,
  isSameDay,
  isWithinInterval,
  subDays,
  startOfWeek as getStartOfWeek,
} from 'date-fns'
import { Button } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import { useMaxForecastDate } from '../../hooks'

interface DateRangePickerProps {
  onClose?: () => void
}

const QUICK_RANGES = [
  { label: 'Today', getValue: () => {
    const today = new Date()
    return { start: today, end: today }
  }},
  { label: 'Yesterday', getValue: () => {
    const yesterday = subDays(new Date(), 1)
    return { start: yesterday, end: yesterday }
  }},
  { label: 'Last 7 days', getValue: () => {
    const today = new Date()
    return { start: subDays(today, 6), end: today }
  }},
  { label: 'Last 14 days', getValue: () => {
    const today = new Date()
    return { start: subDays(today, 13), end: today }
  }},
  { label: 'Last 30 days', getValue: () => {
    const today = new Date()
    return { start: subDays(today, 29), end: today }
  }},
  { label: 'This week', getValue: () => {
    const today = new Date()
    return { start: getStartOfWeek(today, { weekStartsOn: 1 }), end: today }
  }},
  { label: 'Last week', getValue: () => {
    const today = new Date()
    const lastWeekStart = subDays(getStartOfWeek(today, { weekStartsOn: 1 }), 7)
    return { start: lastWeekStart, end: addDays(lastWeekStart, 6) }
  }},
  { label: 'This month', getValue: () => {
    const today = new Date()
    return { start: startOfMonth(today), end: today }
  }},
  { label: 'Last month', getValue: () => {
    const today = new Date()
    const lastMonth = subMonths(today, 1)
    return { start: startOfMonth(lastMonth), end: endOfMonth(lastMonth) }
  }},
]

const WEEKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

export function DateRangePicker({ onClose }: DateRangePickerProps) {
  const { startDate, endDate, setCustomDates } = useFilterStore()
  const maxForecastDate = useMaxForecastDate()

  // Parse existing dates or use today
  const today = new Date()
  const parseDate = (dateStr: string | null) => dateStr ? new Date(dateStr + 'T00:00:00') : null

  const [rangeStart, setRangeStart] = useState<Date | null>(parseDate(startDate))
  const [rangeEnd, setRangeEnd] = useState<Date | null>(parseDate(endDate))
  const [hoverDate, setHoverDate] = useState<Date | null>(null)
  const [currentMonth, setCurrentMonth] = useState(rangeStart || today)
  const [isSelectingEnd, setIsSelectingEnd] = useState(false)

  // Max date for selection
  const maxDate = maxForecastDate ? new Date(maxForecastDate + 'T00:00:00') : today

  // Generate calendar days
  const calendarDays = useMemo(() => {
    const monthStart = startOfMonth(currentMonth)
    const monthEnd = endOfMonth(currentMonth)
    const calendarStart = startOfWeek(monthStart, { weekStartsOn: 1 })
    const calendarEnd = endOfWeek(monthEnd, { weekStartsOn: 1 })

    const days: Date[] = []
    let day = calendarStart
    while (day <= calendarEnd) {
      days.push(day)
      day = addDays(day, 1)
    }
    return days
  }, [currentMonth])

  // Handle day click
  const handleDayClick = useCallback((day: Date) => {
    if (day > maxDate) return

    if (!rangeStart || (rangeStart && rangeEnd)) {
      // Start new selection
      setRangeStart(day)
      setRangeEnd(null)
      setIsSelectingEnd(true)
    } else {
      // Complete selection
      if (day < rangeStart) {
        setRangeEnd(rangeStart)
        setRangeStart(day)
      } else {
        setRangeEnd(day)
      }
      setIsSelectingEnd(false)
    }
  }, [rangeStart, rangeEnd, maxDate])

  // Handle quick range selection
  const handleQuickRange = useCallback((getValue: () => { start: Date; end: Date }) => {
    const { start, end } = getValue()
    const clampedEnd = end > maxDate ? maxDate : end
    setRangeStart(start)
    setRangeEnd(clampedEnd)
    setCurrentMonth(start)
    setIsSelectingEnd(false)
  }, [maxDate])

  // Apply selection
  const handleApply = useCallback(() => {
    if (rangeStart) {
      const endToUse = rangeEnd || rangeStart
      setCustomDates(
        format(rangeStart, 'yyyy-MM-dd'),
        format(endToUse, 'yyyy-MM-dd')
      )
      onClose?.()
    }
  }, [rangeStart, rangeEnd, setCustomDates, onClose])

  // Check if day is in range (for highlighting)
  const isInRange = useCallback((day: Date) => {
    if (!rangeStart) return false
    const end = rangeEnd || (isSelectingEnd ? hoverDate : null)
    if (!end) return false

    const [start, finish] = rangeStart <= end ? [rangeStart, end] : [end, rangeStart]
    return isWithinInterval(day, { start, end: finish })
  }, [rangeStart, rangeEnd, hoverDate, isSelectingEnd])

  const isRangeStart = useCallback((day: Date) => rangeStart && isSameDay(day, rangeStart), [rangeStart])
  const isRangeEnd = useCallback((day: Date) => {
    const end = rangeEnd || (isSelectingEnd ? hoverDate : null)
    return end && isSameDay(day, end)
  }, [rangeEnd, hoverDate, isSelectingEnd])

  // Navigation
  const goToPrevMonth = useCallback(() => setCurrentMonth(prev => subMonths(prev, 1)), [])
  const goToNextMonth = useCallback(() => setCurrentMonth(prev => addMonths(prev, 1)), [])

  const isValid = rangeStart !== null

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-center pt-20 sm:pt-32 bg-black/20"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose?.()
      }}
    >
      <div
        className="bg-white rounded-xl shadow-2xl border border-slate-200 p-4 flex flex-col sm:flex-row gap-4 max-w-[95vw] max-h-[80vh] overflow-auto"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Quick ranges - left side on desktop, top on mobile */}
        <div className="flex sm:flex-col gap-1 sm:gap-0.5 overflow-x-auto sm:overflow-visible pb-2 sm:pb-0 sm:pr-4 sm:border-r border-slate-200 sm:min-w-[120px] flex-shrink-0">
          <div className="hidden sm:block text-xs font-medium text-slate-500 mb-2">Quick select</div>
          {QUICK_RANGES.map(({ label, getValue }) => (
            <button
              key={label}
              onClick={() => handleQuickRange(getValue)}
              className="px-3 py-1.5 text-xs sm:text-sm text-left text-slate-600 hover:bg-slate-100 rounded-lg whitespace-nowrap transition-colors"
            >
              {label}
            </button>
          ))}
        </div>

        {/* Calendar */}
        <div className="flex-1 min-w-[280px]">
          {/* Month navigation */}
          <div className="flex items-center justify-between mb-4">
            <button
              onClick={goToPrevMonth}
              className="p-1.5 hover:bg-slate-100 rounded-lg transition-colors"
              aria-label="Previous month"
            >
              <svg className="w-5 h-5 text-slate-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
              </svg>
            </button>
            <span className="font-semibold text-slate-800">
              {format(currentMonth, 'MMMM yyyy')}
            </span>
            <button
              onClick={goToNextMonth}
              className="p-1.5 hover:bg-slate-100 rounded-lg transition-colors"
              aria-label="Next month"
            >
              <svg className="w-5 h-5 text-slate-600" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
              </svg>
            </button>
          </div>

          {/* Weekday headers */}
          <div className="grid grid-cols-7 gap-1 mb-1">
            {WEEKDAYS.map((day) => (
              <div key={day} className="text-center text-xs font-medium text-slate-400 py-1">
                {day}
              </div>
            ))}
          </div>

          {/* Calendar grid */}
          <div className="grid grid-cols-7 gap-0.5">
            {calendarDays.map((day, idx) => {
              const isCurrentMonth = isSameMonth(day, currentMonth)
              const isToday = isSameDay(day, today)
              const isDisabled = day > maxDate
              const inRange = isInRange(day)
              const isStart = isRangeStart(day)
              const isEnd = isRangeEnd(day)

              return (
                <button
                  key={idx}
                  onClick={() => handleDayClick(day)}
                  onMouseEnter={() => isSelectingEnd && setHoverDate(day)}
                  onMouseLeave={() => setHoverDate(null)}
                  disabled={isDisabled}
                  className={`
                    relative h-9 w-full text-sm transition-all
                    ${!isCurrentMonth ? 'text-slate-300' : 'text-slate-700'}
                    ${isDisabled ? 'text-slate-200 cursor-not-allowed' : 'hover:bg-blue-50 cursor-pointer'}
                    ${isToday && !isStart && !isEnd ? 'font-bold text-blue-600' : ''}
                    ${inRange && !isStart && !isEnd ? 'bg-blue-100' : ''}
                    ${isStart || isEnd ? 'bg-blue-500 text-white hover:bg-blue-600 font-medium z-10' : ''}
                    ${isStart ? 'rounded-l-lg' : ''}
                    ${isEnd ? 'rounded-r-lg' : ''}
                    ${isStart && isEnd ? 'rounded-lg' : ''}
                    ${inRange && !isStart && !isEnd ? 'rounded-none' : ''}
                  `}
                >
                  {format(day, 'd')}
                </button>
              )
            })}
          </div>

          {/* Selected range display */}
          <div className="mt-4 pt-3 border-t border-slate-200">
            <div className="flex items-center justify-between text-sm">
              <div className="text-slate-600">
                {rangeStart ? (
                  <>
                    <span className="font-medium text-slate-800">{format(rangeStart, 'MMM d, yyyy')}</span>
                    {rangeEnd && !isSameDay(rangeStart, rangeEnd) && (
                      <>
                        <span className="mx-2 text-slate-400">→</span>
                        <span className="font-medium text-slate-800">{format(rangeEnd, 'MMM d, yyyy')}</span>
                      </>
                    )}
                  </>
                ) : (
                  <span className="text-slate-400 italic">Click a date to start</span>
                )}
              </div>
            </div>
          </div>

          {/* Actions */}
          <div className="flex items-center gap-2 mt-4">
            <Button
              size="sm"
              variant="primary"
              onClick={handleApply}
              disabled={!isValid}
              className="flex-1"
            >
              Apply
            </Button>
            {onClose && (
              <Button
                size="sm"
                variant="ghost"
                onClick={onClose}
                className="flex-1"
              >
                Cancel
              </Button>
            )}
          </div>
        </div>
      </div>
    </div>
  )
}
