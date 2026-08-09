import { useCallback, useEffect, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { Button } from './Button'
import { useAuth } from '../hooks/useAuth'
import { useFilterStore } from '../store/filterStore'
import type { SalesType } from '../types/filters'

// `internal` is staff outside the retail list and the wholesale manager —
// one manager's own sales, another's shipments to bloggers. Admin-only, and
// the API enforces that independently: hiding the button is not a control.
const SALES_TYPES: { value: SalesType; labelKey: string; adminOnly?: boolean }[] = [
  { value: 'retail', labelKey: 'filter.retail' },
  { value: 'b2b', labelKey: 'filter.b2b' },
  { value: 'internal', labelKey: 'filter.internal', adminOnly: true },
  { value: 'all', labelKey: 'filter.all' },
]

export function SalesTypeFilter() {
  const { t } = useTranslation()
  const { salesType, setSalesType } = useFilterStore()
  const { user } = useAuth()
  const isAdmin = user?.role === 'admin'

  const visibleTypes = useMemo(
    () => SALES_TYPES.filter((type) => !type.adminOnly || isAdmin),
    [isAdmin],
  )

  // A viewer whose stored filter says `internal` — demoted since, or a shared
  // link — would otherwise sit on a selection they cannot see and every
  // request would 403.
  useEffect(() => {
    if (!isAdmin && salesType === 'internal') {
      setSalesType('retail')
    }
  }, [isAdmin, salesType, setSalesType])

  const handleChange = useCallback((type: SalesType) => {
    setSalesType(type)
  }, [setSalesType])

  return (
    <div className="flex items-center gap-0.5 bg-slate-100/80 rounded-lg sm:rounded-xl p-0.5 sm:p-1 border border-slate-200/60">
      {visibleTypes.map(({ value, labelKey }) => (
        <Button
          key={value}
          size="pill"
          variant={salesType === value ? 'primary' : 'ghost'}
          onClick={() => handleChange(value)}
        >
          {t(labelKey)}
        </Button>
      ))}
    </div>
  )
}
