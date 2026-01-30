import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../api/client'
import { useQueryParams, useFilterStore } from '../store/filterStore'
import type { FilterStore } from '../types/filters'

// Stable selector to prevent unnecessary re-renders
const selectSalesType = (s: FilterStore) => s.salesType
import type {
  SummaryResponse,
  RevenueTrendResponse,
  SalesBySourceResponse,
  TopProductsResponse,
  ProductPerformanceResponse,
  CustomerInsightsResponse,
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
  revenueTrend: (params: string) => ['revenueTrend', params] as const,
  salesBySource: (params: string) => ['salesBySource', params] as const,
  topProducts: (params: string) => ['topProducts', params] as const,
  productPerformance: (params: string) => ['productPerformance', params] as const,
  customerInsights: (params: string) => ['customerInsights', params] as const,
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

// ─── Summary ────────────────────────────────────────────────────────────────

export function useSummary() {
  const queryParams = useQueryParams()

  return useQuery<SummaryResponse>({
    queryKey: queryKeys.summary(queryParams),
    queryFn: () => api.getSummary(queryParams),
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
  })
}

// ─── Forecast ──────────────────────────────────────────────────────────────

export function useMaxForecastDate() {
  const salesType = useFilterStore(selectSalesType)

  const { data } = useQuery({
    queryKey: ['maxForecastDate', salesType],
    queryFn: () => api.getRevenueForecast(salesType),
    staleTime: 10 * 60 * 1000,
  })

  return data?.forecast_end ?? data?.month_end ?? null
}

// ─── Sales ──────────────────────────────────────────────────────────────────

export function useSalesBySource() {
  const queryParams = useQueryParams()

  return useQuery<SalesBySourceResponse>({
    queryKey: queryKeys.salesBySource(queryParams),
    queryFn: () => api.getSalesBySource(queryParams),
  })
}

// ─── Products ───────────────────────────────────────────────────────────────

export function useTopProducts() {
  const queryParams = useQueryParams()

  return useQuery<TopProductsResponse>({
    queryKey: queryKeys.topProducts(queryParams),
    queryFn: () => api.getTopProducts(queryParams),
  })
}

export function useProductPerformance() {
  const queryParams = useQueryParams()

  return useQuery<ProductPerformanceResponse>({
    queryKey: queryKeys.productPerformance(queryParams),
    queryFn: () => api.getProductPerformance(queryParams),
  })
}

// ─── Categories ─────────────────────────────────────────────────────────────

export function useCategories() {
  return useQuery<Category[]>({
    queryKey: queryKeys.categories(),
    queryFn: () => api.getCategories(),
    staleTime: 10 * 60 * 1000, // Categories rarely change
  })
}

export function useChildCategories(parentId: number | null) {
  return useQuery<Category[]>({
    queryKey: queryKeys.childCategories(parentId!),
    queryFn: () => api.getChildCategories(parentId!),
    enabled: parentId !== null,
    staleTime: 10 * 60 * 1000,
  })
}

export function useCategoryBreakdown(parentCategory: string | null) {
  const queryParams = useQueryParams()

  return useQuery({
    queryKey: queryKeys.categoryBreakdown(parentCategory!, queryParams),
    queryFn: () => api.getCategoryBreakdown(parentCategory!, queryParams),
    enabled: parentCategory !== null,
  })
}

// ─── Customers ──────────────────────────────────────────────────────────────

export function useCustomerInsights() {
  const queryParams = useQueryParams()

  return useQuery<CustomerInsightsResponse>({
    queryKey: queryKeys.customerInsights(queryParams),
    queryFn: () => api.getCustomerInsights(queryParams),
  })
}

// ─── Brands ─────────────────────────────────────────────────────────────────

export function useBrands() {
  return useQuery<Brand[]>({
    queryKey: queryKeys.brands(),
    queryFn: () => api.getBrands(),
    staleTime: 10 * 60 * 1000,
  })
}

export function useBrandAnalytics() {
  const queryParams = useQueryParams()

  return useQuery<BrandAnalyticsResponse>({
    queryKey: queryKeys.brandAnalytics(queryParams),
    queryFn: () => api.getBrandAnalytics(queryParams),
  })
}

// ─── Expenses ───────────────────────────────────────────────────────────────

export function useExpenseTypes() {
  return useQuery<ExpenseType[]>({
    queryKey: queryKeys.expenseTypes(),
    queryFn: () => api.getExpenseTypes(),
    staleTime: 10 * 60 * 1000,
  })
}

export function useExpenseSummary() {
  const queryParams = useQueryParams()

  return useQuery<ExpenseSummaryResponse>({
    queryKey: queryKeys.expenseSummary(queryParams),
    queryFn: () => api.getExpenseSummary(queryParams),
  })
}

export function useProfitAnalysis() {
  const queryParams = useQueryParams()

  return useQuery<ProfitAnalysisResponse>({
    queryKey: queryKeys.profitAnalysis(queryParams),
    queryFn: () => api.getProfitAnalysis(queryParams),
  })
}

// ─── Goals ─────────────────────────────────────────────────────────────────

export function useGoals() {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<GoalsResponse>({
    queryKey: queryKeys.goals(salesType),
    queryFn: () => api.getGoals(salesType),
    staleTime: 5 * 60 * 1000, // Goals don't change often
  })
}

export function useGoalHistory(periodType: string, weeksBack = 4) {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<GoalHistoryResponse>({
    queryKey: queryKeys.goalHistory(periodType, weeksBack, salesType),
    queryFn: () => api.getGoalHistory(periodType, weeksBack, salesType),
    staleTime: 5 * 60 * 1000,
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
    staleTime: 5 * 60 * 1000, // Goals don't change often
  })
}

export function useSeasonality() {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<Record<number, SeasonalityIndex>>({
    queryKey: queryKeys.seasonality(salesType),
    queryFn: () => api.getSeasonality(salesType),
    staleTime: 30 * 60 * 1000, // Seasonality rarely changes
  })
}

export function useGrowthMetrics() {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<GrowthMetrics>({
    queryKey: queryKeys.growthMetrics(salesType),
    queryFn: () => api.getGrowthMetrics(salesType),
    staleTime: 30 * 60 * 1000,
  })
}

export function useWeeklyPatterns() {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<Record<number, Record<number, number>>>({
    queryKey: queryKeys.weeklyPatterns(salesType),
    queryFn: () => api.getWeeklyPatterns(salesType),
    staleTime: 30 * 60 * 1000,
  })
}

export function useGoalForecast(year: number, month: number) {
  const salesType = useFilterStore(selectSalesType)

  return useQuery<GoalForecastResponse>({
    queryKey: queryKeys.goalForecast(year, month, salesType),
    queryFn: () => api.getGoalForecast(year, month, salesType),
    staleTime: 10 * 60 * 1000,
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
    staleTime: 5 * 60 * 1000, // 5 minutes
  })
}

export function useInventoryTrend(days = 90, granularity: 'daily' | 'monthly' = 'daily') {
  return useQuery<InventoryTrendResponse>({
    queryKey: queryKeys.inventoryTrend(days, granularity),
    queryFn: () => api.getInventoryTrend(days, granularity),
    staleTime: 10 * 60 * 1000, // 10 minutes
  })
}

// V2 Inventory Analysis (view-based)
export function useInventoryAnalysis() {
  return useQuery<InventoryAnalysisResponse>({
    queryKey: queryKeys.inventoryAnalysis(),
    queryFn: () => api.getInventoryAnalysis(),
    staleTime: 10 * 60 * 1000,
  })
}

export function useStockActions() {
  return useQuery<StockAction[]>({
    queryKey: queryKeys.stockActions(),
    queryFn: () => api.getStockActions(),
    staleTime: 10 * 60 * 1000,
  })
}

export function useRestockAlerts() {
  return useQuery<RestockAlert[]>({
    queryKey: queryKeys.restockAlerts(),
    queryFn: () => api.getRestockAlerts(),
    staleTime: 5 * 60 * 1000,
  })
}
