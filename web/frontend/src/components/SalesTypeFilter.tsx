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
  // The offline store (source 7) is retail sold over a counter, and a category
  // of its own by the owner's decision of 2026-11-15 — beside retail, visible
  // to every viewer, and not gated by the API.
  { value: 'offline', labelKey: 'filter.offline' },
  { value: 'b2b', labelKey: 'filter.b2b' },
  // Виставка (source 5) is a real sales channel, not staff activity, so it is
  // visible to every viewer — and the API does not gate it either.
  { value: 'exhibition', labelKey: 'filter.exhibition' },
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

  // Scrolls sideways when the row is narrower than the buttons — a phone, in
  // Ukrainian or Russian — rather than running over what stands beside it.
  return (
    <div className="flex w-fit max-w-full overflow-x-auto scrollbar-hide items-center gap-0.5 bg-slate-100/80 rounded-lg sm:rounded-xl p-0.5 sm:p-1 border border-slate-200/60">
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
