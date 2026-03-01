/**
 * API Client with timeout, abort support, and enhanced error handling.
 */

import type {
  SummaryResponse,
  ReturnsResponse,
  RevenueTrendResponse,
  RevenueForecast,
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
  CategoryBreakdown,
  Category,
  Brand,
  ExpenseType,
  HealthResponse,
  GoalsResponse,
  GoalHistoryResponse,
  SetGoalResponse,
  SmartGoalsResponse,
  SeasonalityIndex,
  GrowthMetrics,
  GoalForecastResponse,
  StockSummaryResponse,
  InventoryTrendResponse,
  InventoryAnalysisResponse,
  InventoryTurnoverResponse,
  StockAction,
  RestockAlert,
  CurrentUserResponse,
  AdminUsersResponse,
  AdminUser,
  UserRole,
  UserStatus,
  PermissionsMatrixResponse,
  UpdatePermissionResponse,
  TrafficAnalyticsResponse,
  TrafficTrendResponse,
  TrafficTransactionsResponse,
  TrafficROASResponse,
  CreateExpenseRequest,
  CreateExpenseResponse,
  DeleteExpenseResponse,
  ReportSummaryResponse,
  ReportTopProductsResponse,
  BasketSummaryResponse,
  ProductPair,
  BasketBucket,
  CategoryCombo,
  BrandAffinityPair,
  ProductMomentumResponse,
} from '../types/api'

// ─── Configuration ───────────────────────────────────────────────────────────

const API_BASE = '/api'
const DEFAULT_TIMEOUT = 10000 // 10 seconds (reduced from 30s - faster failure detection)

// ─── Request Deduplication ──────────────────────────────────────────────────
// Prevents duplicate requests when users rapidly click filters

const pendingRequests = new Map<string, Promise<unknown>>()

function dedupKey(endpoint: string, queryParams?: string): string {
  return queryParams ? `${endpoint}?${queryParams}` : endpoint
}

// ─── Error Classes ───────────────────────────────────────────────────────────

export class ApiError extends Error {
  status: number
  code: string

  constructor(status: number, message: string, code = 'API_ERROR') {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }

  static fromResponse(response: Response): ApiError {
    const status = response.status

    // Map common HTTP statuses to meaningful messages
    const messages: Record<number, string> = {
      400: 'Invalid request parameters',
      401: 'Authentication required',
      403: 'Access denied',
      404: 'Resource not found',
      429: 'Too many requests, please slow down',
      500: 'Server error, please try again',
      502: 'Server is restarting, please wait...',
      503: 'Service temporarily unavailable',
      504: 'Server is taking too long to respond',
    }

    const message = messages[status] || `API error: ${response.statusText}`
    const code = status >= 500 ? 'SERVER_ERROR' : 'CLIENT_ERROR'

    return new ApiError(status, message, code)
  }
}

export class NetworkError extends Error {
  code: string

  constructor(message: string, code = 'NETWORK_ERROR') {
    super(message)
    this.name = 'NetworkError'
    this.code = code
  }
}

export class TimeoutError extends Error {
  constructor(timeout: number) {
    super(`Request timed out after ${timeout}ms`)
    this.name = 'TimeoutError'
  }
}

// ─── Fetch with Timeout ──────────────────────────────────────────────────────

interface FetchOptions {
  timeout?: number
  signal?: AbortSignal
}

async function fetchWithTimeout(
  url: string,
  options: FetchOptions = {}
): Promise<Response> {
  const { timeout = DEFAULT_TIMEOUT, signal: externalSignal } = options

  // Create abort controller for timeout
  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeout)

  // Combine external signal with timeout signal
  const signal = externalSignal
    ? combineSignals(externalSignal, controller.signal)
    : controller.signal

  try {
    const response = await fetch(url, { signal })
    clearTimeout(timeoutId)
    return response
  } catch (error) {
    clearTimeout(timeoutId)

    if (error instanceof Error) {
      if (error.name === 'AbortError') {
        // Check if it was our timeout or external abort
        if (controller.signal.aborted && !externalSignal?.aborted) {
          throw new TimeoutError(timeout)
        }
        throw error // Re-throw external abort
      }

      // Network errors (offline, DNS failure, etc.)
      throw new NetworkError(
        navigator.onLine
          ? 'Unable to connect to server'
          : 'No internet connection',
        navigator.onLine ? 'CONNECTION_FAILED' : 'OFFLINE'
      )
    }

    throw error
  }
}

/**
 * Combines multiple AbortSignals into one.
 * Uses native AbortSignal.any() for proper cleanup (no memory leaks).
 */
function combineSignals(...signals: AbortSignal[]): AbortSignal {
  // Use native AbortSignal.any() if available (modern browsers)
  // This properly handles cleanup when signals are no longer needed
  if ('any' in AbortSignal) {
    return AbortSignal.any(signals)
  }

  // Fallback for older browsers (Safari < 17.4, Firefox < 124)
  const controller = new AbortController()

  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort(signal.reason)
      break
    }
    signal.addEventListener('abort', () => controller.abort(signal.reason), { once: true })
  }

  return controller.signal
}

// ─── Core Fetch Function ─────────────────────────────────────────────────────

async function fetchApi<T>(
  endpoint: string,
  queryParams?: string,
  options?: FetchOptions
): Promise<T> {
  const url = queryParams
    ? `${API_BASE}${endpoint}?${queryParams}`
    : `${API_BASE}${endpoint}`

  // Request deduplication - prevent duplicate in-flight requests
  const key = dedupKey(endpoint, queryParams)
  const pending = pendingRequests.get(key)
  if (pending) {
    return pending as Promise<T>
  }

  const request = (async () => {
    const response = await fetchWithTimeout(url, options)

    if (!response.ok) {
      throw ApiError.fromResponse(response)
    }

    try {
      return await response.json()
    } catch {
      throw new ApiError(response.status, 'Invalid response format', 'PARSE_ERROR')
    }
  })()

  pendingRequests.set(key, request)

  try {
    return await request
  } finally {
    pendingRequests.delete(key)
  }
}

async function fetchApiMutation<T>(
  endpoint: string,
  method: 'POST' | 'DELETE' | 'PATCH',
  options?: FetchOptions
): Promise<T> {
  const { timeout = DEFAULT_TIMEOUT, signal: externalSignal } = options || {}

  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeout)

  const signal = externalSignal
    ? combineSignals(externalSignal, controller.signal)
    : controller.signal

  try {
    const response = await fetch(`${API_BASE}${endpoint}`, { method, signal })
    clearTimeout(timeoutId)

    if (!response.ok) {
      throw ApiError.fromResponse(response)
    }

    return await response.json()
  } catch (error) {
    clearTimeout(timeoutId)

    if (error instanceof ApiError) {
      throw error
    }

    if (error instanceof Error) {
      if (error.name === 'AbortError') {
        if (controller.signal.aborted && !externalSignal?.aborted) {
          throw new TimeoutError(timeout)
        }
        throw error
      }

      throw new NetworkError(
        navigator.onLine ? 'Unable to connect to server' : 'No internet connection',
        navigator.onLine ? 'CONNECTION_FAILED' : 'OFFLINE'
      )
    }

    throw error
  }
}

async function fetchApiMutationWithBody<T>(
  endpoint: string,
  method: 'POST' | 'DELETE' | 'PATCH',
  body: unknown,
  options?: FetchOptions
): Promise<T> {
  const { timeout = DEFAULT_TIMEOUT, signal: externalSignal } = options || {}

  const controller = new AbortController()
  const timeoutId = setTimeout(() => controller.abort(), timeout)

  const signal = externalSignal
    ? combineSignals(externalSignal, controller.signal)
    : controller.signal

  try {
    const response = await fetch(`${API_BASE}${endpoint}`, {
      method,
      signal,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
    clearTimeout(timeoutId)

    if (!response.ok) {
      throw ApiError.fromResponse(response)
    }

    return await response.json()
  } catch (error) {
    clearTimeout(timeoutId)

    if (error instanceof ApiError) {
      throw error
    }

    if (error instanceof Error) {
      if (error.name === 'AbortError') {
        if (controller.signal.aborted && !externalSignal?.aborted) {
          throw new TimeoutError(timeout)
        }
        throw error
      }

      throw new NetworkError(
        navigator.onLine ? 'Unable to connect to server' : 'No internet connection',
        navigator.onLine ? 'CONNECTION_FAILED' : 'OFFLINE'
      )
    }

    throw error
  }
}

// ─── API Client ──────────────────────────────────────────────────────────────

export const api = {
  // Summary
  getSummary: (params: string, options?: FetchOptions) =>
    fetchApi<SummaryResponse>('/summary', params, options),

  // Returns
  getReturns: (params: string, options?: FetchOptions) =>
    fetchApi<ReturnsResponse>('/returns', params, options),

  // Revenue
  getRevenueTrend: (params: string, options?: FetchOptions) =>
    fetchApi<RevenueTrendResponse>('/revenue/trend', params, options),

  getRevenueForecast: (salesType: string, options?: FetchOptions) =>
    fetchApi<RevenueForecast>('/revenue/forecast', `sales_type=${salesType}`, options),

  // Sales
  getSalesBySource: (params: string, options?: FetchOptions) =>
    fetchApi<SalesBySourceResponse>('/sales/by-source', params, options),

  // Products
  getTopProducts: (params: string, options?: FetchOptions) =>
    fetchApi<TopProductsResponse>('/products/top', params, options),

  getProductPerformance: (params: string, options?: FetchOptions) =>
    fetchApi<ProductPerformanceResponse>('/products/performance', params, options),

  // Categories
  getCategories: (options?: FetchOptions) =>
    fetchApi<Category[]>('/categories', undefined, options),

  getChildCategories: (parentId: number, options?: FetchOptions) =>
    fetchApi<Category[]>(`/categories/${parentId}/children`, undefined, options),

  getCategoryBreakdown: (parentCategory: string, params: string, options?: FetchOptions) =>
    fetchApi<CategoryBreakdown>(
      '/categories/breakdown',
      `${params}&parent_category=${encodeURIComponent(parentCategory)}`,
      options
    ),

  // Customers
  getCustomerInsights: (params: string, options?: FetchOptions) =>
    fetchApi<CustomerInsightsResponse>('/customers/insights', params, options),

  getCohortRetention: (
    monthsBack = 12,
    retentionMonths = 6,
    salesType = 'retail',
    includeRevenue = true,
    options?: FetchOptions
  ) =>
    fetchApi<EnhancedCohortRetentionResponse>(
      '/customers/cohort-retention',
      `months_back=${monthsBack}&retention_months=${retentionMonths}&sales_type=${salesType}&include_revenue=${includeRevenue}`,
      options
    ),

  getPurchaseTiming: (
    monthsBack = 12,
    salesType = 'retail',
    options?: FetchOptions
  ) =>
    fetchApi<PurchaseTimingResponse>(
      '/customers/purchase-timing',
      `months_back=${monthsBack}&sales_type=${salesType}`,
      options
    ),

  getCohortLTV: (
    monthsBack = 12,
    retentionMonths = 12,
    salesType = 'retail',
    options?: FetchOptions
  ) =>
    fetchApi<CohortLTVResponse>(
      '/customers/cohort-ltv',
      `months_back=${monthsBack}&retention_months=${retentionMonths}&sales_type=${salesType}`,
      options
    ),

  getAtRiskCustomers: (
    daysThreshold = 90,
    monthsBack = 12,
    salesType = 'retail',
    options?: FetchOptions
  ) =>
    fetchApi<AtRiskResponse>(
      '/customers/at-risk',
      `days_threshold=${daysThreshold}&months_back=${monthsBack}&sales_type=${salesType}`,
      options
    ),

  // Brands
  getBrands: (options?: FetchOptions) =>
    fetchApi<Brand[]>('/brands', undefined, options),

  getBrandAnalytics: (params: string, options?: FetchOptions) =>
    fetchApi<BrandAnalyticsResponse>('/brands/analytics', params, options),

  // Expenses
  getExpenseTypes: (options?: FetchOptions) =>
    fetchApi<ExpenseType[]>('/expense-types', undefined, options),

  getExpenseSummary: (params: string, options?: FetchOptions) =>
    fetchApi<ExpenseSummaryResponse>('/expenses/summary', params, options),

  getProfitAnalysis: (params: string, options?: FetchOptions) =>
    fetchApi<ProfitAnalysisResponse>('/expenses/profit', params, options),

  // Health
  getHealth: (options?: FetchOptions) =>
    fetchApi<HealthResponse>('/health', undefined, options),

  // Goals
  getGoals: (salesType = 'retail', options?: FetchOptions) =>
    fetchApi<GoalsResponse>('/goals', `sales_type=${salesType}`, options),

  getGoalHistory: (
    periodType: string,
    weeksBack = 4,
    salesType = 'retail',
    options?: FetchOptions
  ) =>
    fetchApi<GoalHistoryResponse>(
      '/goals/history',
      `period_type=${periodType}&weeks_back=${weeksBack}&sales_type=${salesType}`,
      options
    ),

  setGoal: (
    periodType: string,
    amount: number,
    growthFactor = 1.10,
    options?: FetchOptions
  ): Promise<SetGoalResponse> =>
    fetchApiMutation<SetGoalResponse>(
      `/goals?period_type=${periodType}&amount=${amount}&growth_factor=${growthFactor}`,
      'POST',
      options
    ),

  resetGoal: (
    periodType: string,
    salesType = 'retail',
    options?: FetchOptions
  ): Promise<SetGoalResponse> =>
    fetchApiMutation<SetGoalResponse>(
      `/goals/${periodType}?sales_type=${salesType}`,
      'DELETE',
      options
    ),

  // Smart Goals
  getSmartGoals: (salesType = 'retail', options?: FetchOptions) =>
    fetchApi<SmartGoalsResponse>('/goals/smart', `sales_type=${salesType}`, options),

  getSeasonality: (salesType = 'retail', options?: FetchOptions) =>
    fetchApi<Record<number, SeasonalityIndex>>('/goals/seasonality', `sales_type=${salesType}`, options),

  getGrowthMetrics: (salesType = 'retail', options?: FetchOptions) =>
    fetchApi<GrowthMetrics>('/goals/growth', `sales_type=${salesType}`, options),

  getWeeklyPatterns: (salesType = 'retail', options?: FetchOptions) =>
    fetchApi<Record<number, Record<number, number>>>('/goals/weekly-patterns', `sales_type=${salesType}`, options),

  getGoalForecast: (
    year: number,
    month: number,
    salesType = 'retail',
    recalculate = false,
    options?: FetchOptions
  ) =>
    fetchApi<GoalForecastResponse>(
      '/goals/forecast',
      `year=${year}&month=${month}&sales_type=${salesType}&recalculate=${recalculate}`,
      options
    ),

  recalculateSeasonality: (
    salesType = 'retail',
    options?: FetchOptions
  ): Promise<{ status: string; message: string; summary: { monthsCalculated: number; overallYoY: number; yearsAnalyzed: number } }> =>
    fetchApiMutation(
      `/goals/recalculate?sales_type=${salesType}`,
      'POST',
      options
    ),

  // Stocks
  getStockSummary: (limit = 20, options?: FetchOptions) =>
    fetchApi<StockSummaryResponse>('/stocks/summary', `limit=${limit}`, options),

  getInventoryTrend: (days = 90, granularity: 'daily' | 'monthly' = 'daily', options?: FetchOptions) =>
    fetchApi<InventoryTrendResponse>('/stocks/trend', `days=${days}&granularity=${granularity}`, options),

  getInventoryTurnover: (params: {
    days?: number; leadTime?: number; safetyMultiplier?: number;
    bufferDays?: number; maxAcceptableDays?: number;
  } = {}, options?: FetchOptions) => {
    const p = new URLSearchParams()
    p.set('days', String(params.days ?? 30))
    if (params.leadTime != null) p.set('lead_time', String(params.leadTime))
    if (params.safetyMultiplier != null) p.set('safety_multiplier', String(params.safetyMultiplier))
    if (params.bufferDays != null) p.set('buffer_days', String(params.bufferDays))
    if (params.maxAcceptableDays != null) p.set('max_acceptable_days', String(params.maxAcceptableDays))
    return fetchApi<InventoryTurnoverResponse>('/stocks/turnover', p.toString(), options)
  },

  // V2 Inventory Analysis (view-based)
  getInventoryAnalysis: (options?: FetchOptions) =>
    fetchApi<InventoryAnalysisResponse>('/stocks/analysis', undefined, options),

  getStockActions: (options?: FetchOptions) =>
    fetchApi<StockAction[]>('/stocks/actions', undefined, options),

  getRestockAlerts: (options?: FetchOptions) =>
    fetchApi<RestockAlert[]>('/stocks/alerts', undefined, options),

  // Auth
  getCurrentUser: (options?: FetchOptions) =>
    fetchApi<CurrentUserResponse>('/me', undefined, options),

  updatePreferences: (body: { language: string }, options?: FetchOptions) =>
    fetchApiMutationWithBody<{ language: string }>('/me/preferences', 'PATCH', body, options),

  // Admin User Management
  getAdminUsers: (
    status?: UserStatus,
    role?: UserRole,
    limit = 100,
    offset = 0,
    options?: FetchOptions
  ) => {
    const params = new URLSearchParams()
    if (status) params.append('status', status)
    if (role) params.append('role', role)
    params.append('limit', String(limit))
    params.append('offset', String(offset))
    return fetchApi<AdminUsersResponse>('/admin/users', params.toString(), options)
  },

  getAdminUser: (userId: number, options?: FetchOptions) =>
    fetchApi<{ user: AdminUser }>(`/admin/users/${userId}`, undefined, options),

  updateUserRole: (userId: number, role: UserRole, options?: FetchOptions) =>
    fetchApiMutation<{ success: boolean; user_id: number; role: UserRole }>(
      `/admin/users/${userId}/role?role=${role}`,
      'PATCH',
      options
    ),

  updateUserStatus: (userId: number, status: UserStatus, options?: FetchOptions) =>
    fetchApiMutation<{ success: boolean; user_id: number; status: UserStatus }>(
      `/admin/users/${userId}/status?status=${status}`,
      'PATCH',
      options
    ),

  // Admin Permissions Management
  getPermissionsMatrix: (options?: FetchOptions) =>
    fetchApi<PermissionsMatrixResponse>('/admin/permissions', undefined, options),

  updatePermission: (
    role: UserRole,
    feature: string,
    canView: boolean,
    canEdit: boolean,
    canDelete: boolean,
    options?: FetchOptions
  ) =>
    fetchApiMutation<UpdatePermissionResponse>(
      `/admin/permissions?role=${role}&feature=${feature}&can_view=${canView}&can_edit=${canEdit}&can_delete=${canDelete}`,
      'PATCH',
      options
    ),

  // Traffic Analytics
  getTrafficAnalytics: (params: string, options?: FetchOptions) =>
    fetchApi<TrafficAnalyticsResponse>('/traffic/analytics', params, options),

  getTrafficTrend: (params: string, options?: FetchOptions) =>
    fetchApi<TrafficTrendResponse>('/traffic/trend', params, options),

  getTrafficTransactions: (params: string, options?: FetchOptions) =>
    fetchApi<TrafficTransactionsResponse>('/traffic/transactions', params, options),

  getTrafficROAS: (params: string, options?: FetchOptions) =>
    fetchApi<TrafficROASResponse>('/traffic/roas', params, options),

  createExpense: (data: CreateExpenseRequest, options?: FetchOptions) =>
    fetchApiMutationWithBody<CreateExpenseResponse>('/expenses', 'POST', data, options),

  deleteExpense: (id: number, options?: FetchOptions) =>
    fetchApiMutation<DeleteExpenseResponse>(`/expenses/${id}`, 'DELETE', options),

  // Reports
  getReportSummary: (params: string, options?: FetchOptions) =>
    fetchApi<ReportSummaryResponse>('/reports/summary', params, options),

  getReportTopProducts: (params: string, options?: FetchOptions) =>
    fetchApi<ReportTopProductsResponse>('/reports/top-products', params, options),

  // Product Intelligence
  getBasketSummary: (params: string, options?: FetchOptions) =>
    fetchApi<BasketSummaryResponse>('/products/intel/summary', params, options),

  getProductPairs: (params: string, options?: FetchOptions) =>
    fetchApi<ProductPair[]>('/products/intel/pairs', params, options),

  getBasketDistribution: (params: string, options?: FetchOptions) =>
    fetchApi<BasketBucket[]>('/products/intel/basket-distribution', params, options),

  getCategoryCombos: (params: string, options?: FetchOptions) =>
    fetchApi<CategoryCombo[]>('/products/intel/category-combos', params, options),

  getBrandAffinity: (params: string, options?: FetchOptions) =>
    fetchApi<BrandAffinityPair[]>('/products/intel/brand-affinity', params, options),

  getProductMomentum: (params: string, options?: FetchOptions) =>
    fetchApi<ProductMomentumResponse>('/products/intel/momentum', params, options),
}

// ─── Type Exports ────────────────────────────────────────────────────────────

export type { FetchOptions }
