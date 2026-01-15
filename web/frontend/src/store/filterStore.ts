import { create } from 'zustand'
import type { FilterStore, Period, SalesType } from '../types/filters'

const initialState = {
  period: 'week' as Period,
  startDate: null,
  endDate: null,
  salesType: 'retail' as SalesType,
  sourceId: null,
  categoryId: null,
  brand: null,
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

  resetFilters: () =>
    set(initialState),
}))

// Selector to build query string from filter state
export const useQueryParams = () => {
  const { period, startDate, endDate, salesType, sourceId, categoryId, brand } =
    useFilterStore()

  const params = new URLSearchParams()

  if (period !== 'custom') {
    params.set('period', period)
  } else if (startDate && endDate) {
    params.set('start_date', startDate)
    params.set('end_date', endDate)
  }

  params.set('sales_type', salesType)

  if (sourceId) params.set('source_id', String(sourceId))
  if (categoryId) params.set('category_id', String(categoryId))
  if (brand) params.set('brand', brand)

  return params.toString()
}
