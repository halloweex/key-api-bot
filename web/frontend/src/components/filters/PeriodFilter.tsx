import { useState, useCallback, useRef, useEffect } from 'react'
import { Button } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import { DateRangePicker } from './DateRangePicker'
import type { Period } from '../../types/filters'
import { format } from 'date-fns'

// Quick access periods shown as buttons
const QUICK_PERIODS: { value: Period; label: string; mobileLabel: string }[] = [
  { value: 'today', label: 'Today', mobileLabel: 'Today' },
  { value: 'yesterday', label: 'Yesterday', mobileLabel: 'Yest.' },
  { value: 'last_7_days', label: '7 Days', mobileLabel: '7d' },
  { value: 'last_28_days', label: '28 Days', mobileLabel: '28d' },
  { value: 'month', label: 'This Month', mobileLabel: 'Month' },
]

// Periods in the "More" dropdown
const MORE_PERIODS: { value: Period; label: string }[] = [
  { value: 'week', label: 'This Week' },
  { value: 'last_week', label: 'Last Week' },
  { value: 'last_month', label: 'Last Month' },
]

export function PeriodFilter() {
  const { period, startDate, endDate, setPeriod } = useFilterStore()
  const [showDatePicker, setShowDatePicker] = useState(false)
  const [showMoreMenu, setShowMoreMenu] = useState(false)
  const moreMenuRef = useRef<HTMLDivElement>(null)

  const handlePeriodChange = useCallback((newPeriod: Period) => {
    setPeriod(newPeriod)
    setShowDatePicker(false)
    setShowMoreMenu(false)
  }, [setPeriod])

  const handleCustomClick = useCallback(() => {
    setShowDatePicker((prev) => !prev)
    setShowMoreMenu(false)
  }, [])

  const handleDatePickerClose = useCallback(() => {
    setShowDatePicker(false)
  }, [])

  // Close more menu on click outside
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (moreMenuRef.current && !moreMenuRef.current.contains(e.target as Node)) {
        setShowMoreMenu(false)
      }
    }
    if (showMoreMenu) {
      document.addEventListener('mousedown', handleClickOutside)
    }
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [showMoreMenu])

  // Check if current period is in "More" menu
  const isMorePeriodActive = MORE_PERIODS.some(p => p.value === period)
  const activeMoreLabel = MORE_PERIODS.find(p => p.value === period)?.label

  // Format custom date range for button label
  const customLabel = period === 'custom' && startDate && endDate
    ? startDate === endDate
      ? format(new Date(startDate), 'MMM d')
      : `${format(new Date(startDate), 'MMM d')} - ${format(new Date(endDate), 'MMM d')}`
    : 'Custom'

  return (
    <div className="relative flex flex-col sm:flex-row items-stretch sm:items-center gap-2 sm:gap-3">
      <div className="flex items-center gap-1 sm:gap-0.5 bg-slate-100/80 rounded-xl p-1 sm:p-1 border border-slate-200/60 flex-shrink-0">
        {/* Quick period buttons */}
        {QUICK_PERIODS.map(({ value, label, mobileLabel }) => (
          <Button
            key={value}
            size="sm"
            variant={period === value ? 'primary' : 'ghost'}
            onClick={() => handlePeriodChange(value)}
            className={`${period === value ? 'shadow-sm' : ''} whitespace-nowrap text-xs sm:text-xs md:text-sm px-2.5 sm:px-3 py-1.5 min-h-[36px] sm:min-h-[32px] rounded-lg`}
          >
            <span className="sm:hidden">{mobileLabel}</span>
            <span className="hidden sm:inline">{label}</span>
          </Button>
        ))}

        {/* More dropdown */}
        <div ref={moreMenuRef} className="relative">
          <Button
            size="sm"
            variant={isMorePeriodActive ? 'primary' : 'ghost'}
            onClick={() => setShowMoreMenu(prev => !prev)}
            className={`${isMorePeriodActive ? 'shadow-sm' : ''} whitespace-nowrap text-xs sm:text-xs md:text-sm px-2.5 sm:px-3 py-1.5 min-h-[36px] sm:min-h-[32px] rounded-lg flex items-center gap-1`}
          >
            <span>{isMorePeriodActive ? activeMoreLabel : 'More'}</span>
            <svg className={`w-3 h-3 transition-transform ${showMoreMenu ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </Button>

          {/* Dropdown menu */}
          {showMoreMenu && (
            <div className="absolute top-full left-0 mt-1 bg-white rounded-lg shadow-lg border border-slate-200 py-1 z-50 min-w-[140px] animate-fade-in">
              {MORE_PERIODS.map(({ value, label }) => (
                <button
                  key={value}
                  onClick={() => handlePeriodChange(value)}
                  className={`w-full text-left px-3 py-2 text-sm hover:bg-slate-50 transition-colors ${
                    period === value ? 'text-purple-600 font-medium bg-purple-50' : 'text-slate-700'
                  }`}
                >
                  {label}
                </button>
              ))}
            </div>
          )}
        </div>

        {/* Custom date button */}
        <Button
          size="sm"
          variant={period === 'custom' ? 'primary' : 'ghost'}
          onClick={handleCustomClick}
          className={`${period === 'custom' ? 'shadow-sm' : ''} whitespace-nowrap text-xs sm:text-xs md:text-sm px-2.5 sm:px-3 py-1.5 min-h-[36px] sm:min-h-[32px] rounded-lg flex items-center gap-1.5`}
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
