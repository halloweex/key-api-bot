/**
 * Lazy-loaded chart components for code splitting.
 * Reduces initial bundle size by loading charts on demand.
 */

import { lazy } from 'react'

// ─── Lazy Chart Components ───────────────────────────────────────────────────

export const LazyRevenueTrendChart = lazy(() =>
  import('./RevenueTrendChart').then(m => ({ default: m.RevenueTrendChart }))
)

export const LazySalesBySourceChart = lazy(() =>
  import('./SalesBySourceChart').then(m => ({ default: m.SalesBySourceChart }))
)

export const LazyOrdersBySourceChart = lazy(() =>
  import('./OrdersBySourceChart').then(m => ({ default: m.OrdersBySourceChart }))
)

export const LazyRevenueBySourceChart = lazy(() =>
  import('./RevenueBySourceChart').then(m => ({ default: m.RevenueBySourceChart }))
)

export const LazyTopProductsChart = lazy(() =>
  import('./TopProductsChart').then(m => ({ default: m.TopProductsChart }))
)

export const LazyTopProductsByRevenueChart = lazy(() =>
  import('./TopProductsByRevenueChart').then(m => ({ default: m.TopProductsByRevenueChart }))
)

export const LazyCategoryChart = lazy(() =>
  import('./CategoryChart').then(m => ({ default: m.CategoryChart }))
)

export const LazyCustomerInsightsChart = lazy(() =>
  import('./CustomerInsightsChart').then(m => ({ default: m.CustomerInsightsChart }))
)

export const LazyCohortRetentionChart = lazy(() =>
  import('./CohortRetentionChart').then(m => ({ default: m.CohortRetentionChart }))
)

export const LazyBrandAnalyticsChart = lazy(() =>
  import('./BrandAnalyticsChart').then(m => ({ default: m.BrandAnalyticsChart }))
)

export const LazyExpensesChart = lazy(() =>
  import('./ExpensesChart').then(m => ({ default: m.ExpensesChart }))
)

export const LazyStockSummaryChart = lazy(() =>
  import('./StockSummaryChart').then(m => ({ default: m.StockSummaryChart }))
)

export const LazyDeadStockChart = lazy(() =>
  import('./DeadStockChart').then(m => ({ default: m.DeadStockChart }))
)

export const LazyInventoryTrendChart = lazy(() =>
  import('./InventoryTrendChart').then(m => ({ default: m.InventoryTrendChart }))
)

export const LazyManualExpensesTable = lazy(() =>
  import('./ManualExpensesTable').then(m => ({ default: m.ManualExpensesTable }))
)
