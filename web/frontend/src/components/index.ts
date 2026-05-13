// Top-level barrel for the flat components/ folder.
// One component = one file = one import path. Consumers can either
// `import { X } from '../components'` (via this barrel) or
// `import { X } from '../components/X'` (direct).

// ─── Primitives ──────────────────────────────────────────────────────────────
export { Badge } from './Badge'
export { Button } from './Button'
export { Card, CardHeader, CardTitle, CardContent } from './Card'
export { Checkbox } from './Checkbox'
export { ExportCsvButton } from './ExportCsvButton'
export { FilterChip } from './FilterChip'
export { InfoPopover } from './InfoPopover'
export { Input } from './Input'
export { MetricCard } from './MetricCard'
export { Select } from './Select'
export { Textarea } from './Textarea'
export { Wrapper } from './Wrapper'

// ─── UI scenes ───────────────────────────────────────────────────────────────
export { ApiErrorState } from './ApiErrorState'
export { ErrorBoundary } from './ErrorBoundary'
export { LiveIndicator } from './LiveIndicator'
export { LottieAnimation } from './LottieAnimation'
export { MilestoneProgress } from './MilestoneProgress'
export { ProtectedSection } from './ProtectedSection'
export { ROICalculator } from './ROICalculator'
export {
  SkeletonCard,
  SkeletonChart,
  SkeletonHorizontalBars,
  SkeletonTable,
  SkeletonMomentum,
  SkeletonRetentionMatrix,
  SkeletonVerticalBars,
} from './Skeleton'
export { ToastProvider, useToast, createToastHelpers } from './Toast'
export { UserAvatar } from './UserAvatar'
export { UserProfileDropdown } from './UserProfileDropdown'
export { VirtualList } from './VirtualList'

// ─── Layout / navigation ─────────────────────────────────────────────────────
export { Dashboard } from './Dashboard'
export { Header } from './Header'
export { NavLink } from './NavLink'
export { SidebarRail } from './SidebarRail'

// ─── Admin ───────────────────────────────────────────────────────────────────
export { AdminUsersPage } from './AdminUsersPage'
export { AdminPermissionsPage } from './AdminPermissionsPage'

// ─── Chat ────────────────────────────────────────────────────────────────────
export { ChatSidebar } from './ChatSidebar'
export { ChatToggle } from './ChatToggle'

// ─── Cards / metrics ─────────────────────────────────────────────────────────
export { StatCard } from './StatCard'
export { SummaryCards } from './SummaryCards'

// ─── Filters ─────────────────────────────────────────────────────────────────
export { BrandFilter } from './BrandFilter'
export { CategoryFilter } from './CategoryFilter'
export { DateRangePicker } from './DateRangePicker'
export { FilterBar } from './FilterBar'
export { PeriodFilter } from './PeriodFilter'
export { PromocodeFilter } from './PromocodeFilter'
export { SalesTypeFilter } from './SalesTypeFilter'
export { SourceFilter } from './SourceFilter'

// ─── Charts (eager) ──────────────────────────────────────────────────────────
export { ChartContainer } from './ChartContainer'
export { CohortRetentionChart } from './CohortRetentionChart'
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

// ─── Charts (lazy chunks) ────────────────────────────────────────────────────
export {
  LazyRevenueTrendChart,
  LazySalesBySourceChart,
  LazyOrdersBySourceChart,
  LazyRevenueBySourceChart,
  LazyTopProductsChart,
  LazyTopProductsByRevenueChart,
  LazyCategoryChart,
  LazyCustomerInsightsChart,
  LazyCohortRetentionChart,
  LazyBrandAnalyticsChart,
  LazyExpensesChart,
  LazyStockSummaryChart,
  LazyDeadStockChart,
  LazyInventoryTrendChart,
  LazyInventoryTurnoverChart,
  LazyBrandRotationCard,
  LazySkuRotationTable,
  LazyPromocodeAnalyticsChart,
  LazyManualExpensesTable,
} from './chartsLazy'

// ─── Pages ───────────────────────────────────────────────────────────────────
export { InventoryPage } from './InventoryPage'
export { MarketingPage } from './MarketingPage'
export { MarginPage } from './MarginPage'
export { ProductIntelPage } from './ProductIntelPage'
export { ReportsPage } from './ReportsPage'
export { TrafficPage } from './TrafficPage'
