export {
  // Query keys
  queryKeys,
  // Summary & Revenue
  useSummary,
  useRevenueTrend,
  useMaxForecastDate,
  // Sales
  useSalesBySource,
  // Products
  useTopProducts,
  useProductPerformance,
  // Categories
  useCategories,
  useChildCategories,
  useCategoryBreakdown,
  // Customers
  useCustomerInsights,
  useCohortRetention,
  // Brands
  useBrands,
  useBrandAnalytics,
  // Expenses
  useExpenseTypes,
  useExpenseSummary,
  useProfitAnalysis,
  // Goals
  useGoals,
  useGoalHistory,
  useSetGoal,
  useResetGoal,
  // Smart Goals
  useSmartGoals,
  useSeasonality,
  useGrowthMetrics,
  useWeeklyPatterns,
  useGoalForecast,
  useRecalculateSeasonality,
  // Stock
  useStockSummary,
  useInventoryTrend,
  // V2 Inventory Analysis
  useInventoryAnalysis,
  useStockActions,
  useRestockAlerts,
} from './useApi'

// Shared chart hooks
export { useSourceChartData } from './useSourceChartData'
export type { SourceChartDataPoint, SourceChartData } from './useSourceChartData'
