import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { api } from '../api/client'
import { useQueryParams, useFilterStore } from '../store/filterStore'
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
} from '../types/api'

// Query key factory for consistent cache keys
export const queryKeys = {
  // Goals
  goals: (salesType: string) => ['goals', salesType] as const,
  goalHistory: (periodType: string, salesType: string) => ['goalHistory', periodType, salesType] as const,
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

export function useRevenueTrend() {
  const queryParams = useQueryParams()

  return useQuery<RevenueTrendResponse>({
    queryKey: queryKeys.revenueTrend(queryParams),
    queryFn: () => api.getRevenueTrend(queryParams),
  })
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
  const salesType = useFilterStore((s) => s.salesType)

  return useQuery<GoalsResponse>({
    queryKey: queryKeys.goals(salesType),
    queryFn: () => api.getGoals(salesType),
    staleTime: 5 * 60 * 1000, // Goals don't change often
  })
}

export function useGoalHistory(periodType: string, weeksBack = 4) {
  const salesType = useFilterStore((s) => s.salesType)

  return useQuery<GoalHistoryResponse>({
    queryKey: queryKeys.goalHistory(periodType, salesType),
    queryFn: () => api.getGoalHistory(periodType, weeksBack, salesType),
    staleTime: 5 * 60 * 1000,
  })
}

export function useSetGoal() {
  const queryClient = useQueryClient()
  const salesType = useFilterStore((s) => s.salesType)

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
  const salesType = useFilterStore((s) => s.salesType)

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
  const salesType = useFilterStore((s) => s.salesType)

  return useQuery<SmartGoalsResponse>({
    queryKey: queryKeys.smartGoals(salesType),
    queryFn: () => api.getSmartGoals(salesType),
    staleTime: 5 * 60 * 1000, // Goals don't change often
  })
}

export function useSeasonality() {
  const salesType = useFilterStore((s) => s.salesType)

  return useQuery<Record<number, SeasonalityIndex>>({
    queryKey: queryKeys.seasonality(salesType),
    queryFn: () => api.getSeasonality(salesType),
    staleTime: 30 * 60 * 1000, // Seasonality rarely changes
  })
}

export function useGrowthMetrics() {
  const salesType = useFilterStore((s) => s.salesType)

  return useQuery<GrowthMetrics>({
    queryKey: queryKeys.growthMetrics(salesType),
    queryFn: () => api.getGrowthMetrics(salesType),
    staleTime: 30 * 60 * 1000,
  })
}

export function useWeeklyPatterns() {
  const salesType = useFilterStore((s) => s.salesType)

  return useQuery<Record<number, Record<number, number>>>({
    queryKey: queryKeys.weeklyPatterns(salesType),
    queryFn: () => api.getWeeklyPatterns(salesType),
    staleTime: 30 * 60 * 1000,
  })
}

export function useGoalForecast(year: number, month: number) {
  const salesType = useFilterStore((s) => s.salesType)

  return useQuery<GoalForecastResponse>({
    queryKey: queryKeys.goalForecast(year, month, salesType),
    queryFn: () => api.getGoalForecast(year, month, salesType),
    staleTime: 10 * 60 * 1000,
    enabled: year > 0 && month > 0,
  })
}

export function useRecalculateSeasonality() {
  const queryClient = useQueryClient()
  const salesType = useFilterStore((s) => s.salesType)

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
