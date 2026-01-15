const API_BASE = '/api'

export class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function fetchApi<T>(endpoint: string, queryParams?: string): Promise<T> {
  const url = queryParams
    ? `${API_BASE}${endpoint}?${queryParams}`
    : `${API_BASE}${endpoint}`

  const response = await fetch(url)

  if (!response.ok) {
    throw new ApiError(response.status, `API error: ${response.statusText}`)
  }

  return response.json()
}

export const api = {
  // Summary
  getSummary: (params: string) =>
    fetchApi<import('../types/api').SummaryResponse>('/summary', params),

  // Revenue
  getRevenueTrend: (params: string) =>
    fetchApi<import('../types/api').RevenueTrendResponse>('/revenue/trend', params),

  // Sales
  getSalesBySource: (params: string) =>
    fetchApi<import('../types/api').SalesBySourceResponse>('/sales/by-source', params),

  // Products
  getTopProducts: (params: string) =>
    fetchApi<import('../types/api').TopProductsResponse>('/products/top', params),

  getProductPerformance: (params: string) =>
    fetchApi<import('../types/api').ProductPerformanceResponse>('/products/performance', params),

  // Categories
  getCategories: () =>
    fetchApi<import('../types/api').Category[]>('/categories'),

  getChildCategories: (parentId: number) =>
    fetchApi<import('../types/api').Category[]>(`/categories/${parentId}/children`),

  getCategoryBreakdown: (parentCategory: string, params: string) =>
    fetchApi<import('../types/api').CategoryBreakdown>(
      '/categories/breakdown',
      `${params}&parent_category=${encodeURIComponent(parentCategory)}`
    ),

  // Customers
  getCustomerInsights: (params: string) =>
    fetchApi<import('../types/api').CustomerInsightsResponse>('/customers/insights', params),

  // Brands
  getBrands: () =>
    fetchApi<import('../types/api').Brand[]>('/brands'),

  getBrandAnalytics: (params: string) =>
    fetchApi<import('../types/api').BrandAnalyticsResponse>('/brands/analytics', params),

  // Expenses
  getExpenseTypes: () =>
    fetchApi<import('../types/api').ExpenseType[]>('/expense-types'),

  getExpenseSummary: (params: string) =>
    fetchApi<import('../types/api').ExpenseSummaryResponse>('/expenses/summary', params),

  getProfitAnalysis: (params: string) =>
    fetchApi<import('../types/api').ProfitAnalysisResponse>('/expenses/profit', params),

  // Health
  getHealth: () =>
    fetchApi<import('../types/api').HealthResponse>('/health'),
}
