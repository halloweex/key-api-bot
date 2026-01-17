import { useCallback } from 'react'
import { Button } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import type { SalesType } from '../../types/filters'

const SALES_TYPES: { value: SalesType; label: string }[] = [
  { value: 'retail', label: 'Retail' },
  { value: 'b2b', label: 'B2B' },
  { value: 'all', label: 'All' },
]

export function SalesTypeFilter() {
  const { salesType, setSalesType } = useFilterStore()

  const handleChange = useCallback((type: SalesType) => {
    setSalesType(type)
  }, [setSalesType])

  return (
    <div className="flex items-center gap-1 bg-slate-100 rounded-lg p-1">
      {SALES_TYPES.map(({ value, label }) => (
        <Button
          key={value}
          size="sm"
          variant={salesType === value ? 'primary' : 'ghost'}
          onClick={() => handleChange(value)}
        >
          {label}
        </Button>
      ))}
    </div>
  )
}
