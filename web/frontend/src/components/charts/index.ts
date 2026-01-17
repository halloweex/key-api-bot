// Configuration
export * from './config'

// Components (eager loading)
export { ChartContainer } from './ChartContainer'
export { RevenueTrendChart } from './RevenueTrendChart'
export { SalesBySourceChart } from './SalesBySourceChart'
export { OrdersBySourceChart } from './OrdersBySourceChart'
export { RevenueBySourceChart } from './RevenueBySourceChart'
export { TopProductsChart } from './TopProductsChart'
export { TopProductsByRevenueChart } from './TopProductsByRevenueChart'
export { CategoryChart } from './CategoryChart'
export { CustomerInsightsChart } from './CustomerInsightsChart'
export { BrandAnalyticsChart } from './BrandAnalyticsChart'
export { ExpensesChart } from './ExpensesChart'

// Lazy components (code splitting)
export {
  LazyRevenueTrendChart,
  LazySalesBySourceChart,
  LazyOrdersBySourceChart,
  LazyRevenueBySourceChart,
  LazyTopProductsChart,
  LazyTopProductsByRevenueChart,
  LazyCategoryChart,
  LazyCustomerInsightsChart,
  LazyBrandAnalyticsChart,
  LazyExpensesChart,
} from './lazy'
