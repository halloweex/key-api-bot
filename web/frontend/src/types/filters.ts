export type Period = 'today' | 'yesterday' | 'week' | 'last_week' | 'month' | 'last_month' | 'last_7_days' | 'last_28_days' | 'all_time' | 'custom'

export type SalesType = 'retail' | 'b2b' | 'exhibition' | 'internal' | 'all'

/** Whose visits the site funnel counts: Ukraine with the visitors GA4 could
 *  not place, or every country, bots included (`/api/traffic/funnel?country=`). */
export type FunnelCountry = 'ua' | 'all'

export interface FilterState {
  period: Period
  startDate: string | null
  endDate: string | null
  salesType: SalesType
  sourceId: number | null
  categoryId: number | null
  brand: string | null
  promocode: string | null
  funnelCountry: FunnelCountry
}

export interface FilterActions {
  setPeriod: (period: Period) => void
  setCustomDates: (startDate: string, endDate: string) => void
  setSalesType: (salesType: SalesType) => void
  setSourceId: (sourceId: number | null) => void
  setCategoryId: (categoryId: number | null) => void
  setBrand: (brand: string | null) => void
  setPromocode: (promocode: string | null) => void
  setFunnelCountry: (country: FunnelCountry) => void
  resetFilters: () => void
}

export type FilterStore = FilterState & FilterActions
