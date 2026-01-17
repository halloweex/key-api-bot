// API Response Types

export interface SummaryResponse {
  totalOrders: number
  totalRevenue: number
  avgCheck: number
  totalReturns: number
  returnRate: number
  startDate: string
  endDate: string
}

export interface RevenueTrendResponse {
  labels: string[]
  revenue: number[]
  orders: number[]
  comparison?: {
    labels: string[]
    revenue: number[]
    orders: number[]
  }
}

export interface SalesBySourceResponse {
  labels: string[]
  revenue: number[]
  orders: number[]
  backgroundColor: string[]
}

export interface TopProductsResponse {
  labels: string[]
  data: number[]
  percentages: number[]
  backgroundColor: string
}

export interface CategoryBreakdown {
  labels: string[]
  revenue: number[]
  quantity: number[]
  backgroundColor: string[]
  parentCategory?: string
}

export interface ProductPerformanceResponse {
  topByRevenue: {
    labels: string[]
    data: number[]
    quantities: number[]
    backgroundColor: string
  }
  categoryBreakdown: CategoryBreakdown
  metrics: {
    totalProducts: number
    totalRevenue: number
    totalQuantity: number
    avgProductRevenue: number
  }
}

export interface CustomerInsightsResponse {
  newVsReturning: {
    labels: string[]
    data: number[]
    backgroundColor: string[]
  }
  aovTrend: {
    labels: string[]
    datasets: Array<{
      label: string
      data: number[]
      borderColor?: string
      backgroundColor?: string
      fill?: boolean
      tension?: number
    }>
  }
  metrics: {
    newCustomers: number
    returningCustomers: number
    repeatRate: number
    averageOrderValue: number
  }
}

export interface BrandAnalyticsResponse {
  topByRevenue: {
    labels: string[]
    data: number[]
    backgroundColor: string
  }
  topByQuantity: {
    labels: string[]
    data: number[]
    backgroundColor: string
  }
  metrics: {
    totalBrands: number
    topBrand: string
    topBrandShare: number
  }
}

export interface ExpenseSummaryResponse {
  byType: {
    labels: string[]
    data: number[]
    backgroundColor: string[]
  }
  trend: {
    labels: string[]
    data: number[]
  }
  metrics: {
    totalExpenses: number
    grossProfit: number
    profitMargin: number
    ordersWithExpenses: number
  }
}

export interface ProfitAnalysisResponse {
  labels: string[]
  revenue: number[]
  expenses: number[]
  profit: number[]
}

export interface Category {
  id: number
  name: string
}

export interface Brand {
  name: string
}

export interface ExpenseType {
  id: number
  name: string
  alias: string
}

export interface HealthResponse {
  status: string
  version: string
  uptime_seconds: number
  duckdb: {
    status: string
    orders?: number
    products?: number
    categories?: number
    db_size_mb?: number
  }
}
