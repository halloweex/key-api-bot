export {
  // Query keys
  queryKeys,
  // Summary & Revenue
  useSummary,
  useReturns,
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
  usePurchaseTiming,
  useCohortLTV,
  useAtRiskCustomers,
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
  // Traffic Analytics
  useTrafficAnalytics,
  useTrafficTrend,
  useTrafficTransactions,
  useTrafficROAS,
  useCreateExpense,
  useDeleteExpense,
} from './useApi'

// Shared chart hooks
export { useSourceChartData } from './useSourceChartData'
export type { SourceChartDataPoint, SourceChartData } from './useSourceChartData'

// WebSocket
export { useWebSocket } from './useWebSocket'
export type { ConnectionState, WebSocketEvent, WebSocketMessage } from './useWebSocket'

// Auth
export {
  useAuth,
  usePermission,
  useRole,
  useIsAdmin,
  useUserDisplayName,
  authQueryKey,
} from './useAuth'

// Router
export { useRouter, navigate } from './useRouter'
