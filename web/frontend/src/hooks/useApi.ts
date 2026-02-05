import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../api/client'
import { useQueryParams, useFilterStore } from '../store/filterStore'
import type { FilterStore } from '../types/filters'

// Stable selector to prevent unnecessary re-renders
const selectSalesType = (s: FilterStore) => s.salesType
import type {
  SummaryResponse,
  ReturnsResponse,
  RevenueTrendResponse,
  SalesBySourceResponse,
  TopProductsResponse,
  ProductPerformanceResponse,
  CustomerInsightsResponse,
  EnhancedCohortRetentionResponse,
  PurchaseTimingResponse,
  CohortLTVResponse,
  AtRiskResponse,
  BrandAnalyticsResponse,
  ExpenseSummaryResponse,
  ProfitAnalysisResponse,
  Category,
  Brand,
  ExpenseType,
  GoalsResponse,
  GoalHistoryResponse,
  SmartGoalsResponse,
  SeasonalityIndex,
  GrowthMetrics,
  GoalForecastResponse,
  StockSummaryResponse,
  InventoryTrendResponse,
  InventoryAnalysisResponse,
  StockAction,
  RestockAlert,
} from '../types/api'

// Query key factory for consistent cache keys
export const queryKeys = {
  // Goals
  goals: (salesType: string) => ['goals', salesType] as const,
  goalHistory: (periodType: string, weeksBack: number, salesType: string) => ['goalHistory', periodType, weeksBack, salesType] as const,
  smartGoals: (salesType: string) => ['smartGoals', salesType] as const,
  seasonality: (salesType: string) => ['seasonality', salesType] as const,
  growthMetrics: (salesType: string) => ['growthMetrics', salesType] as const,
  weeklyPatterns: (salesType: string) => ['weeklyPatterns', salesType] as const,
  goalForecast: (year: number, month: number, salesType: string) => ['goalForecast', year, month, salesType] as const,
  // Data
  summary: (params: string) => ['summary', params] as const,
  returns: (params: string) => ['returns', params] as const,
  revenueTrend: (params: string) => ['revenueTrend', params] as const,
  salesBySource: (params: string) => ['salesBySource', params] as const,
  topProducts: (params: string) => ['topProducts', params] as const,
  productPerformance: (params: string) => ['productPerformance', params] as const,
  customerInsights: (params: string) => ['customerInsights', params] as const,
  cohortRetention: (monthsBack: number, retentionMonths: number, salesType: string, includeRevenue: boolean) =>
    ['cohortRetention', monthsBack, retentionMonths, salesType, includeRevenue] as const,
  purchaseTiming: (monthsBack: number, salesType: string) =>
    ['purchaseTiming', monthsBack, salesType] as const,
  cohortLTV: (monthsBack: number, salesType: string) =>
    ['cohortLTV', monthsBack, salesType] as const,
  atRiskCustomers: (daysThreshold: number, salesType: string) =>
    ['atRiskCustomers', daysThreshold, salesType] as const,
  brandAnalytics: (params: string) => ['brandAnalytics', params] as const,
  expenseSummary: (params: string) => ['expenseSummary', params] as const,
  profitAnalysis: (params: string) => ['profitAnalysis', params] as const,
  categories: () => ['categories'] as const,
  childCategories: (parentId: number) => ['categories', parentId, 'children'] as const,
  categoryBreakdown: (parent: string, params: string) => ['categoryBreakdown', parent, params] as const,
  brands: () => ['brands'] as const,
  expenseTypes: () => ['expenseTypes'] as const,
  stockSummary: (limit: number) => ['stockSummary', limit] as const,
  inventoryTrend: (days: number, granularity: string) => ['inventoryTrend', days, granularity] as const,
  // V2 inventory analysis
  inventoryAnalysis: () => ['inventoryAnalysis'] as const,
  stockActions: () => ['stockActions'] as const,
  restockAlerts: () => ['restockAlerts'] as const,
}

// Cache TTL constants (in milliseconds)
const CACHE_TTL = {
  REALTIME: 2 * 60 * 1000,     // 2 min - summary, revenue, sales (syncs every 60s)
  STANDARD: 5 * 60 * 1000,     // 5 min - products, customers, goals
  STATIC: 10 * 60 * 1000,      // 10 min - categories, brands, expense types
  COMPUTED: 30 * 60 * 1000,    // 30 min - seasonality, growth metrics
}

// ─── Summary ────────────────────────────────────────────────────────────────

export function useSummary() {
  const queryParams = useQueryParams()

  return useQuery<SummaryResponse>({
    queryKey: queryKeys.summary(queryParams),
    queryFn: () => api.getSummary(queryParams),
    staleTime: CACHE_TTL.REALTIME,
  })
}

// ─── Returns ────────────────────────────────────────────────────────────────

export function useReturns(enabled = false) {
  const queryParams = useQueryParams()

  return useQuery<ReturnsResponse>({
    queryKey: queryKeys.returns(queryParams),
    queryFn: () => api.getReturns(queryParams),
    staleTime: CACHE_TTL.REALTIME,
    enabled, // Only fetch when dropdown is open
  })
}

// ─── Revenue ────────────────────────────────────────────────────────────────

export function useRevenueTrend(compareType: string = 'previous_period') {
  const queryParams = useQueryParams()
  const { period, sourceId, categoryId, brand } = useFilterStore()

  // Append compare_type to query params
  let fullParams = compareType !== 'previous_period'
    ? `${queryParams}&compare_type=${compareType}`
    : queryParams

  // Include forecast for periods that contain future days
  const { endDate } = useFilterStore()
  const hasFutureDates = period === 'custom' && endDate && endDate > new Date().toISOString().split('T')[0]
  const isCurrentPeriod = period === 'month' || period === 'week'
  const wantsForecast = (isCurrentPeriod || hasFutureDates) && !sourceId && !categoryId && !brand
  if (wantsForecast) {
    fullParams += '&include_forecast=true'
  }

  return useQuery<RevenueTrendResponse>({
    queryKey: [...queryKeys.revenueTrend(queryParams), compareType, wantsForecast],
    queryFn: () => api.getRevenueTrend(fullParams),
    staleTime: CACHE_TTL.REALTIME,
  })
}

// ─── Forecast ──────────────────────────────────────────────────────────────

export function useMaxForecastDate() {
  const salesType = useFilterStore(selectSalesType)

  const { data } = useQuery({
    queryKey: ['maxForecastDate', salesType],
    queryFn: () => api.getRevenueForecast(salesType),
    staleTime: CACHE_TTL.STATIC,
  })

  return data?.forecast_end ?? data?.month_end ?? null
}

// ─── Sales ──────────────────────────────────────────────────────────────────

export function useSalesBySource() {
  const queryParams = useQueryParams()

  return useQuery<SalesBySourceResponse>({
    queryKey: queryKeys.salesBySource(queryParams),
    queryFn: () => api.getSalesBySource(queryParams),
    staleTime: CACHE_TTL.REALTIME,
  })
}

// ─── Products ───────────────────────────────────────────────────────────────

export function useTopProducts() {
  const queryParams = useQueryParams()

  return useQuery<TopProductsResponse>({
    queryKey: queryKeys.topProducts(queryParams),
    queryFn: () => api.getTopProducts(queryParams),
    staleTime: CACHE_TTL.STANDARD,
  })
}

export function useProductPerformance() {
  const queryParams = useQueryParams()

  return useQuery<ProductPerformanceResponse>({
    queryKey: queryKeys.productPerformance(queryParams),
    queryFn: () => api.getProductPerformance(queryParams),
    staleTime: CACHE_TTL.STANDARD,
  })
}

// ─── Categories ─────────────────────────────────────────────────────────────

export function useCategories() {
  return useQuery<Category[]>({
    queryKey: queryKeys.categories(),
    queryFn: () => api.getCategories(),
    staleTime: CACHE_TTL.STATIC,
  })
}

export function useChildCategories(parentId: number | null) {
  return useQuery<Category[]>({
    queryKey: queryKeys.childCategories(parentId!),
    queryFn: () => api.getChildCategories(parentId!),
    enabled: parentId !== null,
    staleTime: CACHE_TTL.STATIC,
  })
}

export function useCategoryBreakdown(parentCategory: string | null) {
  const queryParams = useQueryParams()

  return useQuery({
    queryKey: queryKeys.categoryBreakdown(parentCategory!, queryParams),
    queryFn: () => api.getCategoryBreakdown(parentCategory!, queryParams),
    enabled: parentCategory !== null,
    staleTime: CACHE_TTL.STANDARD,
  })
}

// ─── Customers ──────────────────────────────────────────────────────────────

export function useCustomerInsights() {
  const queryParams = useQueryParams()

  return useQuery<CustomerInsightsResponse>({
    queryKey: queryKeys.customerInsights(queryParams),
    queryFn: () => api.getCustomerInsights(queryParams),
    staleTime: CACHE_TTL.STANDARD,
  })
}

export function useCohortRetention(
  monthsBack = 12,
  retentionMonths = 6,
  includeRevenue = true
) {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<EnhancedCohortRetentionResponse>({
    queryKey: queryKeys.cohortRetention(monthsBack, retentionMonths, salesType, includeRevenue),
    queryFn: () => api.getCohortRetention(monthsBack, retentionMonths, salesType, includeRevenue),
    staleTime: CACHE_TTL.STANDARD,
  })
}

export function usePurchaseTiming(monthsBack = 12) {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<PurchaseTimingResponse>({
    queryKey: queryKeys.purchaseTiming(monthsBack, salesType),
    queryFn: () => api.getPurchaseTiming(monthsBack, salesType),
    staleTime: CACHE_TTL.STANDARD,
  })
}

export function useCohortLTV(monthsBack = 12) {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<CohortLTVResponse>({
    queryKey: queryKeys.cohortLTV(monthsBack, salesType),
    queryFn: () => api.getCohortLTV(monthsBack, salesType),
    staleTime: CACHE_TTL.STANDARD,
  })
}

export function useAtRiskCustomers(daysThreshold = 90) {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<AtRiskResponse>({
    queryKey: queryKeys.atRiskCustomers(daysThreshold, salesType),
    queryFn: () => api.getAtRiskCustomers(daysThreshold, salesType),
    staleTime: CACHE_TTL.STANDARD,
  })
}

// ─── Brands ─────────────────────────────────────────────────────────────────

export function useBrands() {
  return useQuery<Brand[]>({
    queryKey: queryKeys.brands(),
    queryFn: () => api.getBrands(),
    staleTime: CACHE_TTL.STATIC,
  })
}

export function useBrandAnalytics() {
  const queryParams = useQueryParams()

  return useQuery<BrandAnalyticsResponse>({
    queryKey: queryKeys.brandAnalytics(queryParams),
    queryFn: () => api.getBrandAnalytics(queryParams),
    staleTime: CACHE_TTL.STANDARD,
  })
}

// ─── Expenses ───────────────────────────────────────────────────────────────

export function useExpenseTypes() {
  return useQuery<ExpenseType[]>({
    queryKey: queryKeys.expenseTypes(),
    queryFn: () => api.getExpenseTypes(),
    staleTime: CACHE_TTL.STATIC,
  })
}

export function useExpenseSummary() {
  const queryParams = useQueryParams()

  return useQuery<ExpenseSummaryResponse>({
    queryKey: queryKeys.expenseSummary(queryParams),
    queryFn: () => api.getExpenseSummary(queryParams),
    staleTime: CACHE_TTL.STANDARD,
  })
}

export function useProfitAnalysis() {
  const queryParams = useQueryParams()

  return useQuery<ProfitAnalysisResponse>({
    queryKey: queryKeys.profitAnalysis(queryParams),
    queryFn: () => api.getProfitAnalysis(queryParams),
    staleTime: CACHE_TTL.STANDARD,
  })
}

// ─── Goals ─────────────────────────────────────────────────────────────────

export function useGoals() {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<GoalsResponse>({
    queryKey: queryKeys.goals(salesType),
    queryFn: () => api.getGoals(salesType),
    staleTime: CACHE_TTL.STANDARD,
  })
}

export function useGoalHistory(periodType: string, weeksBack = 4) {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<GoalHistoryResponse>({
    queryKey: queryKeys.goalHistory(periodType, weeksBack, salesType),
    queryFn: () => api.getGoalHistory(periodType, weeksBack, salesType),
    staleTime: CACHE_TTL.STANDARD,
  })
}

export function useSetGoal() {
  const queryClient = useQueryClient()
  const salesType = useFilterStore(selectSalesType)

  return useMutation({
    mutationFn: ({ periodType, amount, growthFactor = 1.10 }: {
      periodType: string
      amount: number
      growthFactor?: number
    }) => api.setGoal(periodType, amount, growthFactor),
    onSuccess: () => {
      // Invalidate goals cache to refetch updated data
      queryClient.invalidateQueries({ queryKey: queryKeys.goals(salesType) })
    },
  })
}

export function useResetGoal() {
  const queryClient = useQueryClient()
  const salesType = useFilterStore(selectSalesType)

  return useMutation({
    mutationFn: (periodType: string) => api.resetGoal(periodType, salesType),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys.goals(salesType) })
      queryClient.invalidateQueries({ queryKey: queryKeys.smartGoals(salesType) })
    },
  })
}

// ─── Smart Goals ────────────────────────────────────────────────────────────

export function useSmartGoals() {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<SmartGoalsResponse>({
    queryKey: queryKeys.smartGoals(salesType),
    queryFn: () => api.getSmartGoals(salesType),
    staleTime: CACHE_TTL.STANDARD,
  })
}

export function useSeasonality() {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<Record<number, SeasonalityIndex>>({
    queryKey: queryKeys.seasonality(salesType),
    queryFn: () => api.getSeasonality(salesType),
    staleTime: CACHE_TTL.COMPUTED,
  })
}

export function useGrowthMetrics() {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<GrowthMetrics>({
    queryKey: queryKeys.growthMetrics(salesType),
    queryFn: () => api.getGrowthMetrics(salesType),
    staleTime: CACHE_TTL.COMPUTED,
  })
}

export function useWeeklyPatterns() {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<Record<number, Record<number, number>>>({
    queryKey: queryKeys.weeklyPatterns(salesType),
    queryFn: () => api.getWeeklyPatterns(salesType),
    staleTime: CACHE_TTL.COMPUTED,
  })
}

export function useGoalForecast(year: number, month: number) {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<GoalForecastResponse>({
    queryKey: queryKeys.goalForecast(year, month, salesType),
    queryFn: () => api.getGoalForecast(year, month, salesType),
    staleTime: CACHE_TTL.STATIC,
    enabled: year > 0 && month > 0,
  })
}

export function useRecalculateSeasonality() {
  const queryClient = useQueryClient()
  const salesType = useFilterStore(selectSalesType)

  return useMutation({
    mutationFn: () => api.recalculateSeasonality(salesType),
    onSuccess: () => {
      // Invalidate all goal-related queries
      queryClient.invalidateQueries({ queryKey: queryKeys.goals(salesType) })
      queryClient.invalidateQueries({ queryKey: queryKeys.smartGoals(salesType) })
      queryClient.invalidateQueries({ queryKey: queryKeys.seasonality(salesType) })
      queryClient.invalidateQueries({ queryKey: queryKeys.growthMetrics(salesType) })
      queryClient.invalidateQueries({ queryKey: queryKeys.weeklyPatterns(salesType) })
    },
  })
}

// ─── Stock ───────────────────────────────────────────────────────────────────

export function useStockSummary(limit = 20) {
  return useQuery<StockSummaryResponse>({
    queryKey: queryKeys.stockSummary(limit),
    queryFn: () => api.getStockSummary(limit),
    staleTime: CACHE_TTL.STANDARD,
  })
}

export function useInventoryTrend(days = 90, granularity: 'daily' | 'monthly' = 'daily') {
  return useQuery<InventoryTrendResponse>({
    queryKey: queryKeys.inventoryTrend(days, granularity),
    queryFn: () => api.getInventoryTrend(days, granularity),
    staleTime: CACHE_TTL.STATIC,
  })
}

// V2 Inventory Analysis (view-based)
export function useInventoryAnalysis() {
  return useQuery<InventoryAnalysisResponse>({
    queryKey: queryKeys.inventoryAnalysis(),
    queryFn: () => api.getInventoryAnalysis(),
    staleTime: CACHE_TTL.STATIC,
  })
}

export function useStockActions() {
  return useQuery<StockAction[]>({
    queryKey: queryKeys.stockActions(),
    queryFn: () => api.getStockActions(),
    staleTime: CACHE_TTL.STATIC,
  })
}

export function useRestockAlerts() {
  return useQuery<RestockAlert[]>({
    queryKey: queryKeys.restockAlerts(),
    queryFn: () => api.getRestockAlerts(),
    staleTime: CACHE_TTL.STANDARD,
  })
}
