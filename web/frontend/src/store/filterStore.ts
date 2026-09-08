import { create } from 'zustand'
import { useMemo } from 'react'
import { useRouter } from '../hooks/useRouter'
import { filtersForPath } from '../utils/pageFilters'
import type { FilterStore, Period, SalesType } from '../types/filters'

const initialState = {
  period: 'month' as Period,
  startDate: null,
  endDate: null,
  salesType: 'retail' as SalesType,
  sourceId: null,
  categoryId: null,
  brand: null,
  promocode: null,
}

export const useFilterStore = create<FilterStore>((set) => ({
  ...initialState,

  setPeriod: (period: Period) =>
    set({ period, startDate: null, endDate: null }),

  setCustomDates: (startDate: string, endDate: string) =>
    set({ period: 'custom', startDate, endDate }),

  setSalesType: (salesType: SalesType) =>
    set({ salesType }),

  setSourceId: (sourceId: number | null) =>
    set({ sourceId }),

  setCategoryId: (categoryId: number | null) =>
    set({ categoryId }),

  setBrand: (brand: string | null) =>
    set({ brand }),

  setPromocode: (promocode: string | null) =>
    set({ promocode }),

  resetFilters: () =>
    set(initialState),
}))

// Selector to build query string from filter state (memoized to prevent re-renders)
export const useQueryParams = () => {
  const { period, startDate, endDate, salesType, sourceId, categoryId, brand, promocode } =
    useFilterStore()
  // A filter the current page's API does not accept is left out rather than
  // sent and ignored — see `utils/pageFilters`. It also keeps the query key
  // honest: two pages that differ only by an ignored parameter were caching
  // under two keys for one answer.
  const path = useRouter()

  // Memoize URLSearchParams construction to prevent unnecessary re-renders
  return useMemo(() => {
    const applies = filtersForPath(path)
    const params = new URLSearchParams()

    if (period !== 'custom') {
      params.set('period', period)
    } else if (startDate && endDate) {
      params.set('start_date', startDate)
      params.set('end_date', endDate)
    }

    params.set('sales_type', salesType)

    if (sourceId && applies.has('sourceId')) params.set('source_id', String(sourceId))
    if (categoryId && applies.has('categoryId')) params.set('category_id', String(categoryId))
    if (brand && applies.has('brand')) params.set('brand', brand)
    if (promocode && applies.has('promocode')) params.set('promocode', promocode)

    return params.toString()
  }, [path, period, startDate, endDate, salesType, sourceId, categoryId, brand, promocode])
}
