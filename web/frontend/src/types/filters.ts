export type Period = 'today' | 'yesterday' | 'week' | 'last_week' | 'month' | 'last_month' | 'last_7_days' | 'last_28_days' | 'custom'

export type SalesType = 'retail' | 'b2b' | 'all'

export interface FilterState {
  period: Period
  startDate: string | null
  endDate: string | null
  salesType: SalesType
  sourceId: number | null
  categoryId: number | null
  brand: string | null
}

export interface FilterActions {
  setPeriod: (period: Period) => void
  setCustomDates: (startDate: string, endDate: string) => void
  setSalesType: (salesType: SalesType) => void
  setSourceId: (sourceId: number | null) => void
  setCategoryId: (categoryId: number | null) => void
  setBrand: (brand: string | null) => void
  resetFilters: () => void
}

export type FilterStore = FilterState & FilterActions
