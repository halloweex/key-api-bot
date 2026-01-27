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
    totalCustomers?: number
    newCustomers: number
    returningCustomers: number
    totalOrders?: number
    repeatRate: number
    averageOrderValue: number
    // CLV metrics
    customerLifetimeValue?: number
    avgPurchaseFrequency?: number
    avgCustomerLifespanDays?: number
    purchaseFrequency?: number
    // All-time metrics
    totalCustomersAllTime?: number
    repeatCustomersAllTime?: number
    trueRepeatRate?: number
    avgOrdersPerCustomer?: number
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
  recent3MonthAvg?: number
  yoyGoal?: number
  recentGoal?: number
  growthRate?: number
  seasonalityIndex?: number
  calculationMethod?: 'yoy_growth' | 'recent_trend' | 'historical_avg' | 'fallback'
}

export interface SmartGoalsResponse {
  daily: SmartGoalData
  weekly: SmartGoalData & { weeklyBreakdown: Record<number, number> }
  monthly: SmartGoalData & {
    lastYearRevenue: number
    recent3MonthAvg: number
    yoyGoal: number
    recentGoal: number
    growthRate: number
    seasonalityIndex: number
    calculationMethod: 'yoy_growth' | 'recent_trend' | 'historical_avg' | 'fallback'
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
    recent3MonthAvg: number
    historicalAvg: number
    yoyGoal: number
    recentGoal: number
    growthRate: number
    seasonalityIndex: number
    confidence: 'high' | 'medium' | 'low'
    calculationMethod: 'yoy_growth' | 'recent_trend' | 'historical_avg' | 'fallback'
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

// Stock types
export interface StockItem {
  sku: string
  quantity: number
  reserve: number
  price: number
  name: string | null
}

export interface OutOfStockItem {
  sku: string
  price: number
  name: string | null
}

export interface StockSummaryResponse {
  summary: {
    totalOffers: number
    inStockCount: number
    outOfStockCount: number
    lowStockCount: number
    totalQuantity: number
    totalReserve: number
    totalValue: number
    reserveValue: number
    averageQuantity: number
    averageValue: number
    avgDataPoints: number
  }
  topByQuantity: StockItem[]
  lowStock: StockItem[]
  outOfStock: OutOfStockItem[]
  lastSync: string | null
}

// Inventory Trend types
export interface InventoryTrendResponse {
  labels: string[]
  quantity: number[]
  value: number[]
  reserve: number[]
  skuCount?: number[]
  valueChange?: number[]
  granularity: 'daily' | 'monthly'
  periodDays: number
  dataPoints: number
  summary?: {
    startValue: number
    endValue: number
    change: number
    changePercent: number
    minValue: number
    maxValue: number
  }
}

// Dead Stock Analysis types
export interface DeadStockStatusSummary {
  skuCount: number
  quantity: number
  value: number
  valuePercent: number
}

export interface CategoryThreshold {
  categoryId: number | null
  categoryName: string
  productsWithSales: number
  medianDays: number
  q1Days: number | null
  q3Days: number | null
  avgDays: number | null
  thresholdDays: number
}

export interface DeadStockItem {
  id: number
  sku: string
  quantity: number
  price: number
  value: number
  name: string | null
  brand: string | null
  categoryId: number | null
  categoryName: string | null
  lastSaleDate: string | null
  daysSinceSale: number | null
  categoryThreshold: number
  status: 'healthy' | 'at_risk' | 'dead_stock' | 'never_sold'
}

export interface DeadStockAnalysisResponse {
  summary: {
    healthy: DeadStockStatusSummary
    atRisk: DeadStockStatusSummary
    deadStock: DeadStockStatusSummary
    neverSold: DeadStockStatusSummary
    total: {
      skuCount: number
      quantity: number
      value: number
    }
  }
  categoryThresholds: CategoryThreshold[]
  items: DeadStockItem[]
  methodology: {
    description: string
    minimumThreshold: number
    defaultThreshold: number
    atRiskMultiplier: number
    minimumSampleSize: number
  }
}
