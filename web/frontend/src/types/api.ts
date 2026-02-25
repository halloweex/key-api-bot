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

export interface ReturnOrder {
  id: number
  date: string
  amount: number
  statusId: number
  statusName: string
  source: string
  buyerId: number | null
  buyerName: string | null
  buyerPhone: string | null
  managerId: number | null
  managerName: string | null
}

export interface ReturnsResponse {
  returns: ReturnOrder[]
  count: number
  startDate: string
  endDate: string
}

export type CompareType = 'previous_period' | 'year_ago' | 'month_ago'

export interface RevenueForecastDaily {
  date: string
  predicted_revenue: number
  model_mae: number
  model_mape: number
}

export interface RevenueForecast {
  actual_to_date: number
  predicted_remaining: number
  predicted_total: number
  daily_predictions: RevenueForecastDaily[]
  model_metrics: {
    mae: number
    mape: number
  }
  last_trained: string | null
  month_start: string
  month_end: string
  forecast_end?: string
}

export interface RevenueTrendResponse {
  labels: string[]
  revenue: number[]
  orders: number[]
  comparison?: {
    labels: string[]
    revenue: number[]
    orders: number[]
    period?: {
      start: string
      end: string
      type: CompareType
    }
    totals?: {
      current: number
      previous: number
      growth_percent: number
    }
  }
  forecast?: RevenueForecast
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

// ─── V2 Inventory Analysis Types ─────────────────────────────────────────────

export interface DeadStockStatusSummary {
  skuCount: number
  quantity: number
  value: number
  valuePercent: number
}

export interface AgingBucket {
  bucket: string
  skuCount: number
  units: number
  value: number
}

export interface CategoryVelocity {
  categoryId: number | null
  categoryName: string
  sampleSize: number
  p50: number | null
  p75: number | null
  p90: number | null
  thresholdDays: number
}

export interface InventoryAnalysisItem {
  id: number
  sku: string
  name: string | null
  brand: string | null
  categoryName: string | null
  quantity: number
  value: number
  price: number
  daysSinceSale: number | null
  daysInStock: number | null
  thresholdDays: number
  status: 'healthy' | 'at_risk' | 'dead_stock' | 'never_sold'
}

export interface InventoryAnalysisResponse {
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
  agingBuckets: AgingBucket[]
  categoryThresholds: CategoryVelocity[]
  items: InventoryAnalysisItem[]
  methodology: {
    description: string
    minimumThreshold: number
    defaultThreshold: number
    atRiskMultiplier: number
  }
}

export interface StockAction {
  offerId: number
  sku: string
  name: string | null
  brand: string | null
  categoryName: string | null
  units: number
  value: number
  daysSinceSale: number | null
  daysInStock: number | null
  status: string
  action: string
}

export interface RestockAlert {
  offerId: number
  sku: string
  name: string | null
  brand: string | null
  unitsLeft: number
  daysSinceSale: number | null
  alertLevel: 'OUT_OF_STOCK' | 'CRITICAL' | 'LOW'
}

// ─── Cohort Retention Types ─────────────────────────────────────────────────

export interface CohortData {
  month: string
  size: number
  retention: (number | null)[]
}

export interface CohortRetentionResponse {
  cohorts: CohortData[]
  retentionMonths: number
  summary: {
    totalCohorts: number
    totalCustomers: number
    avgRetention: Record<number, number>
  }
}

// Enhanced cohort data with revenue tracking
export interface EnhancedCohortData {
  month: string
  size: number
  retention: (number | null)[]         // customer retention %
  revenueRetention: (number | null)[]  // revenue retention %
  revenue: (number | null)[]           // absolute revenue per month
}

export interface EnhancedCohortRetentionResponse {
  cohorts: EnhancedCohortData[]
  retentionMonths: number
  summary: {
    totalCohorts: number
    totalCustomers: number
    avgCustomerRetention: Record<number, number>
    avgRevenueRetention: Record<number, number> | null
    totalRevenue: number | null
  }
}

// Days to second purchase (purchase timing)
export interface PurchaseTimingBucket {
  bucket: string  // "0-30", "31-60", etc.
  customers: number
  avgDays: number
  percentage: number
}

export interface PurchaseTimingResponse {
  buckets: PurchaseTimingBucket[]
  summary: {
    totalRepeatCustomers: number
    medianDays: number | null
    avgDays: number | null
  }
}

// Cohort LTV (lifetime value)
export interface CohortLTVData {
  month: string
  customerCount: number
  cumulativeRevenue: number[]  // cumulative by month index (M0-M12)
  avgLTV: number
}

export interface CohortLTVResponse {
  cohorts: CohortLTVData[]
  summary: {
    avgLTV: number
    bestCohort: string | null
    bestCohortLTV: number
  }
}

// At-risk customers
export interface AtRiskCohort {
  cohort: string
  totalCustomers: number
  atRiskCount: number
  atRiskPct: number
  atRiskRevenue: number
  avgOrdersAtRisk: number
}

export interface AtRiskResponse {
  cohorts: AtRiskCohort[]
  daysThreshold: number
  summary: {
    totalAtRisk: number
    totalCustomers: number
    overallAtRiskPct: number
  }
}

// ─── User & Permissions Types ─────────────────────────────────────────────────

export type UserRole = 'admin' | 'editor' | 'viewer'

export interface User {
  id: number
  username: string
  first_name: string
  last_name: string
  photo_url: string
  role: UserRole
}

export interface FeaturePermissions {
  view: boolean
  edit: boolean
  delete: boolean
}

export interface Permissions {
  dashboard: FeaturePermissions
  expenses: FeaturePermissions
  inventory: FeaturePermissions
  analytics: FeaturePermissions
  customers: FeaturePermissions
  reports: FeaturePermissions
  user_management: FeaturePermissions
}

export interface CurrentUserResponse {
  user: User
  permissions: Permissions
}

// ─── Admin User Management Types ──────────────────────────────────────────────

export type UserStatus = 'pending' | 'approved' | 'denied' | 'frozen'

export interface AdminUser {
  user_id: number
  username: string | null
  first_name: string | null
  last_name: string | null
  photo_url: string | null
  role: UserRole
  status: UserStatus
  requested_at: string | null
  reviewed_at: string | null
  reviewed_by: number | null
  last_activity: string | null
  denial_count: number
  created_at: string | null
}

export interface AdminUsersResponse {
  users: AdminUser[]
  count: number
}

// ─── Admin Permissions Matrix Types ──────────────────────────────────────────

export interface FeatureInfo {
  key: string
  name: string
  description: string
}

export interface RoleInfo {
  key: string
  name: string
  description: string
}

export interface PermissionsMatrixResponse {
  permissions: Record<UserRole, Record<string, FeaturePermissions>>
  features: FeatureInfo[]
  roles: RoleInfo[]
}

export interface UpdatePermissionResponse {
  success: boolean
  role: string
  feature: string
  can_view: boolean
  can_edit: boolean
  can_delete: boolean
}

// ─── Traffic Analytics Types ────────────────────────────────────────────────

export interface TrafficMetric {
  orders: number
  revenue: number
}

export interface TrafficSummary {
  paid: TrafficMetric
  paid_confirmed: TrafficMetric
  paid_likely: TrafficMetric
  organic: TrafficMetric
  manager: TrafficMetric
  pixel_only: TrafficMetric
  unknown: TrafficMetric
}

export interface TrafficAnalyticsResponse {
  period: { start: string; end: string }
  totals: TrafficMetric
  summary: TrafficSummary
  by_platform: Record<string, TrafficMetric>
  by_traffic_type: Record<string, TrafficMetric>
}

export interface TrafficTrendDay {
  date: string
  paid_orders: number
  paid_revenue: number
  organic_orders: number
  organic_revenue: number
  other_orders: number
  other_revenue: number
}

export interface TrafficTrendResponse {
  trend: TrafficTrendDay[]
}

export interface TrafficEvidence {
  field: string
  value: string
  reason?: string
}

export interface TrafficTransaction {
  id: number
  date: string
  amount: number
  source: string
  traffic_type: string
  platform: string
  evidence: TrafficEvidence[]
}

export interface TrafficTransactionsResponse {
  transactions: TrafficTransaction[]
  total: number
  limit: number
  offset: number
}

// ─── ROAS / Ad Spend Types ──────────────────────────────────────────────────

export interface PlatformROAS {
  paid_revenue: number
  spend: number
  roas: number | null
}

export interface TrafficROASResponse {
  blended: { revenue: number; spend: number; roas: number | null }
  by_platform: Record<string, PlatformROAS>
  bonus_tier: string
  has_spend_data: boolean
}

export interface CreateExpenseRequest {
  expense_date: string
  category: string
  expense_type: string
  amount: number
  currency?: string
  note?: string
  platform?: string
}

export interface CreateExpenseResponse {
  id: number
  expense_date: string
  category: string
  expense_type: string
  amount: number
  currency: string
  note: string | null
  created_at: string
  platform: string | null
}

export interface DeleteExpenseResponse {
  success: boolean
  id: number
}

// ─── Report Types ────────────────────────────────────────────────────────────

export interface ReportSourceRow {
  source_id: number
  source_name: string
  orders_count: number
  products_sold: number
  revenue: number
  avg_check: number
  returns_count: number
  return_rate: number
}

export interface ReportSummaryResponse {
  sources: ReportSourceRow[]
  totals: {
    orders_count: number
    products_sold: number
    revenue: number
    avg_check: number
    returns_count: number
    return_rate: number
  }
}

export interface ReportTopProduct {
  rank: number
  product_name: string
  sku: string
  quantity: number
  percentage: number
  revenue: number
  orders_count: number
}

export type ReportTopProductsResponse = ReportTopProduct[]

// ─── Product Intelligence ────────────────────────────────────────────────────

export interface BasketSummaryResponse {
  avgBasketSize: number
  multiItemPct: number
  multiItemOrders: number
  totalOrders: number
  aovUplift: number
  multiAov: number
  singleAov: number
  topPair: string
  topPairCount: number
}

export interface ProductPair {
  productA: { id: number | null; name: string; orders: number }
  productB: { id: number | null; name: string; orders: number }
  coOccurrence: number
  support: number
  confidenceAtoB: number
  confidenceBtoA: number
  lift: number
  totalOrders: number
}

export interface BasketBucket {
  bucket: string
  orders: number
  revenue: number
  aov: number
}

export interface CategoryCombo {
  categoryA: string
  categoryB: string
  coOccurrence: number
}

export interface BrandAffinityPair {
  brandA: string
  brandB: string
  coOccurrence: number
  productPairs: number
}

export interface MomentumProduct {
  productId: number | null
  productName: string
  currentRevenue: number
  prevRevenue: number
  currentQty: number
  prevQty: number
  growthPct: number
}

export interface ProductMomentumResponse {
  gainers: MomentumProduct[]
  losers: MomentumProduct[]
}
