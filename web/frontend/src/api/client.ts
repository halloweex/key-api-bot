/**
 * API Client with timeout, abort support, and enhanced error handling.
 */

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
  DeadStockAnalysisResponse,
} from '../types/api'

// ─── Configuration ───────────────────────────────────────────────────────────

const API_BASE = '/api'
const DEFAULT_TIMEOUT = 30000 // 30 seconds

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
      502: 'Server temporarily unavailable',
      503: 'Service unavailable',
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

  const response = await fetchWithTimeout(url, options)

  if (!response.ok) {
    throw ApiError.fromResponse(response)
  }

  try {
    return await response.json()
  } catch {
    throw new ApiError(response.status, 'Invalid response format', 'PARSE_ERROR')
  }
}

async function fetchApiMutation<T>(
  endpoint: string,
  method: 'POST' | 'DELETE',
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

// ─── API Client ──────────────────────────────────────────────────────────────

export const api = {
  // Summary
  getSummary: (params: string, options?: FetchOptions) =>
    fetchApi<SummaryResponse>('/summary', params, options),

  // Revenue
  getRevenueTrend: (params: string, options?: FetchOptions) =>
    fetchApi<RevenueTrendResponse>('/revenue/trend', params, options),

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

  getDeadStockAnalysis: (options?: FetchOptions) =>
    fetchApi<DeadStockAnalysisResponse>('/stocks/dead', undefined, options),
}

// ─── Type Exports ────────────────────────────────────────────────────────────

export type { FetchOptions }
