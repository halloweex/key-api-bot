import { useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import { Button } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import type { SalesType } from '../../types/filters'

const SALES_TYPES: { value: SalesType; labelKey: string }[] = [
  { value: 'retail', labelKey: 'filter.retail' },
  { value: 'b2b', labelKey: 'filter.b2b' },
  { value: 'all', labelKey: 'filter.all' },
]

export function SalesTypeFilter() {
  const { t } = useTranslation()
  const { salesType, setSalesType } = useFilterStore()

  const handleChange = useCallback((type: SalesType) => {
    setSalesType(type)
  }, [setSalesType])

  return (
    <div className="flex items-center gap-0.5 bg-slate-100/80 rounded-lg sm:rounded-xl p-0.5 sm:p-1 border border-slate-200/60">
      {SALES_TYPES.map(({ value, labelKey }) => (
        <Button
          key={value}
          size="sm"
          variant={salesType === value ? 'primary' : 'ghost'}
          onClick={() => handleChange(value)}
          className={`${salesType === value ? 'shadow-sm' : ''} text-xs sm:text-sm px-2 sm:px-3 py-1 sm:py-1.5`}
        >
          {t(labelKey)}
        </Button>
      ))}
    </div>
  )
}
