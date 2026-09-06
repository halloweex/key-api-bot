/**
 * Lazy-loaded chart components for code splitting.
 * Reduces initial bundle size by loading charts on demand.
 */

import { lazyChunk } from '../utils/lazyChunk'

// ─── Lazy Chart Components ───────────────────────────────────────────────────

export const LazyRevenueTrendChart = lazyChunk(() =>
  import('./RevenueTrendChart').then(m => ({ default: m.RevenueTrendChart }))
)

export const LazySalesBySourceChart = lazyChunk(() =>
  import('./SalesBySourceChart').then(m => ({ default: m.SalesBySourceChart }))
)

export const LazyOrdersBySourceChart = lazyChunk(() =>
  import('./OrdersBySourceChart').then(m => ({ default: m.OrdersBySourceChart }))
)

export const LazyRevenueBySourceChart = lazyChunk(() =>
  import('./RevenueBySourceChart').then(m => ({ default: m.RevenueBySourceChart }))
)

export const LazyTopProductsChart = lazyChunk(() =>
  import('./TopProductsChart').then(m => ({ default: m.TopProductsChart }))
)

export const LazyTopProductsByRevenueChart = lazyChunk(() =>
  import('./TopProductsByRevenueChart').then(m => ({ default: m.TopProductsByRevenueChart }))
)

export const LazyCategoryChart = lazyChunk(() =>
  import('./CategoryChart').then(m => ({ default: m.CategoryChart }))
)

export const LazyCustomerInsightsChart = lazyChunk(() =>
  import('./CustomerInsightsChart').then(m => ({ default: m.CustomerInsightsChart }))
)

export const LazyCohortRetentionChart = lazyChunk(() =>
  import('./CohortRetentionChart').then(m => ({ default: m.CohortRetentionChart }))
)

export const LazyBrandAnalyticsChart = lazyChunk(() =>
  import('./BrandAnalyticsChart').then(m => ({ default: m.BrandAnalyticsChart }))
)

export const LazyExpensesChart = lazyChunk(() =>
  import('./ExpensesChart').then(m => ({ default: m.ExpensesChart }))
)

export const LazyStockSummaryChart = lazyChunk(() =>
  import('./StockSummaryChart').then(m => ({ default: m.StockSummaryChart }))
)

export const LazyDeadStockChart = lazyChunk(() =>
  import('./DeadStockChart').then(m => ({ default: m.DeadStockChart }))
)

export const LazyInventoryTrendChart = lazyChunk(() =>
  import('./InventoryTrendChart').then(m => ({ default: m.InventoryTrendChart }))
)

export const LazyInventoryTurnoverChart = lazyChunk(() =>
  import('./InventoryTurnoverChart').then(m => ({ default: m.InventoryTurnoverChart }))
)

export const LazyBrandRotationCard = lazyChunk(() =>
  import('./BrandRotationCard').then(m => ({ default: m.BrandRotationCard }))
)

export const LazySkuRotationTable = lazyChunk(() =>
  import('./SkuRotationTable').then(m => ({ default: m.SkuRotationTable }))
)

export const LazyPromocodeAnalyticsChart = lazyChunk(() =>
  import('./PromocodeAnalyticsChart').then(m => ({ default: m.PromocodeAnalyticsChart }))
)

export const LazyManualExpensesTable = lazyChunk(() =>
  import('./ManualExpensesTable').then(m => ({ default: m.ManualExpensesTable }))
)
