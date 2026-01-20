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

// ─── Goal Types ─────────────────────────────────────────────────────────────

export interface GoalData {
  amount: number
  isCustom: boolean
  suggestedAmount: number
  basedOnAverage: number
  trend: number
  confidence: 'high' | 'medium' | 'low'
}

export interface GoalsResponse {
  daily: GoalData
  weekly: GoalData
  monthly: GoalData
}

export interface GoalHistoryResponse {
  periodType: string
  average: number
  min: number
  max: number
  periodCount: number
  stdDev: number
  trend: number
  weeksAnalyzed: number
}

export interface SetGoalResponse {
  periodType: string
  amount: number
  isCustom: boolean
  calculatedGoal: number
  growthFactor: number
}

// ─── Smart Goal Types ─────────────────────────────────────────────────────────

export interface SmartGoalData extends GoalData {
  weeklyBreakdown?: Record<number, number>  // week 1-5 -> goal amount
  lastYearRevenue?: number
  growthRate?: number
  seasonalityIndex?: number
  calculationMethod?: 'yoy_growth' | 'historical_avg' | 'fallback'
}

export interface SmartGoalsResponse {
  daily: SmartGoalData
  weekly: SmartGoalData & { weeklyBreakdown: Record<number, number> }
  monthly: SmartGoalData & {
    lastYearRevenue: number
    growthRate: number
    seasonalityIndex: number
    calculationMethod: 'yoy_growth' | 'historical_avg' | 'fallback'
  }
  metadata: {
    overallYoY: number
    monthlyYoY: number
    calculatedAt: string
  }
}

export interface SeasonalityIndex {
  month: number
  avg_revenue: number
  min_revenue: number
  max_revenue: number
  sample_size: number
  std_dev: number
  seasonality_index: number
  confidence: 'high' | 'medium' | 'low'
}

export interface GrowthMetrics {
  overall_yoy: number
  monthly_yoy: Record<number, number>
  yearly_data: Array<{ year: number; revenue: number }>
  sample_size: number
}

export interface GoalForecastResponse {
  targetYear: number
  targetMonth: number
  monthly: {
    goal: number
    lastYearRevenue: number
    historicalAvg: number
    growthRate: number
    seasonalityIndex: number
    confidence: 'high' | 'medium' | 'low'
    calculationMethod: 'yoy_growth' | 'historical_avg' | 'fallback'
  }
  weekly: {
    goal: number
    breakdown: Record<number, number>
    weights: Record<number, number>
  }
  daily: {
    goal: number
    daysInMonth: number
  }
  metadata: {
    overallYoY: number
    monthlyYoY: number
    calculatedAt: string
  }
}
