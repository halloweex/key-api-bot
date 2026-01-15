import { useCallback } from 'react'
import { Button } from '../ui'
import { useFilterStore } from '../../store/filterStore'
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

  const handlePeriodChange = useCallback((newPeriod: Period) => {
    setPeriod(newPeriod)
  }, [setPeriod])

  return (
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
    </div>
  )
}
