import { useQuery } from '@tanstack/react-query'
import { api } from '../api/client'
import { useQueryParams } from '../store/filterStore'
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
} from '../types/api'

// Query key factory for consistent cache keys
export const queryKeys = {
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
