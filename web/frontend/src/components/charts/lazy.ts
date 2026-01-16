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

export const LazyTopProductsChart = lazy(() =>
  import('./TopProductsChart').then(m => ({ default: m.TopProductsChart }))
)

export const LazyCategoryChart = lazy(() =>
  import('./CategoryChart').then(m => ({ default: m.CategoryChart }))
)

export const LazyCustomerInsightsChart = lazy(() =>
  import('./CustomerInsightsChart').then(m => ({ default: m.CustomerInsightsChart }))
)

export const LazyBrandAnalyticsChart = lazy(() =>
  import('./BrandAnalyticsChart').then(m => ({ default: m.BrandAnalyticsChart }))
)

export const LazyExpensesChart = lazy(() =>
  import('./ExpensesChart').then(m => ({ default: m.ExpensesChart }))
)
