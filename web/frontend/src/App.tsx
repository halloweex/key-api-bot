import { lazy, Suspense } from 'react'
import { Dashboard } from './components/Dashboard'
import { AppShell } from './components/AppShell'
import { AdminGuard, PermissionGuard } from './components/RouteGuard'
import { Spinner } from './components/Spinner'
import { useRouter } from './hooks/useRouter'

// Lazy load pages
const AdminUsersPage = lazy(() => import('./components/AdminUsersPage').then(m => ({ default: m.AdminUsersPage })))
const AdminPermissionsPage = lazy(() => import('./components/AdminPermissionsPage').then(m => ({ default: m.AdminPermissionsPage })))
const TrafficPage = lazy(() => import('./components/TrafficPage'))
const ProductIntelPage = lazy(() => import('./components/ProductIntelPage'))
const InventoryPage = lazy(() => import('./components/InventoryPage'))
const ReportsPage = lazy(() => import('./components/ReportsPage'))
const MarketingPage = lazy(() => import('./components/MarketingPage'))
const MarginPage = lazy(() => import('./components/MarginPage'))
const SmsCampaignsPage = lazy(() => import('./components/SmsCampaignsPage'))

// ─── App Component ───────────────────────────────────────────────────────────

function App() {
  const path = useRouter()

  // Admin pages (no AppShell - they have their own layout)
  if (path === '/v2/admin/users' || path === '/admin/users') {
    return (
      <AdminGuard>
        <Suspense fallback={<Spinner />}>
          <AdminUsersPage />
        </Suspense>
      </AdminGuard>
    )
  }

  if (path === '/v2/admin/permissions' || path === '/admin/permissions') {
    return (
      <AdminGuard>
        <Suspense fallback={<Spinner />}>
          <AdminPermissionsPage />
        </Suspense>
      </AdminGuard>
    )
  }

  // Traffic Analytics
  if (path === '/v2/traffic' || path === '/traffic') {
    return (
      <AppShell>
        <Suspense fallback={<Spinner />}>
          <TrafficPage />
        </Suspense>
      </AppShell>
    )
  }

  // Product Intelligence
  if (path === '/v2/products' || path === '/products') {
    return (
      <AppShell>
        <Suspense fallback={<Spinner />}>
          <ProductIntelPage />
        </Suspense>
      </AppShell>
    )
  }

  // Inventory
  if (path === '/v2/inventory' || path === '/inventory') {
    return (
      <AppShell>
        <Suspense fallback={<Spinner />}>
          <InventoryPage />
        </Suspense>
      </AppShell>
    )
  }

  // Marketing
  if (path === '/v2/marketing' || path === '/marketing') {
    return (
      <AppShell>
        <Suspense fallback={<Spinner />}>
          <MarketingPage />
        </Suspense>
      </AppShell>
    )
  }

  // Margin Analysis (admin only — guard inside AppShell to keep sidebar)
  if (path === '/v2/margin' || path === '/margin') {
    return (
      <AppShell>
        <AdminGuard>
          <Suspense fallback={<Spinner />}>
            <MarginPage />
          </Suspense>
        </AdminGuard>
      </AppShell>
    )
  }

  // SMS campaigns (the roster carries names and phone numbers, so it is gated
  // on the `sms` permission — admins and marketers, nobody else)
  if (path === '/v2/sms' || path === '/sms') {
    return (
      <AppShell>
        <PermissionGuard feature="sms">
          <Suspense fallback={<Spinner />}>
            <SmsCampaignsPage />
          </Suspense>
        </PermissionGuard>
      </AppShell>
    )
  }

  // Reports
  if (path === '/v2/reports' || path === '/reports') {
    return (
      <AppShell>
        <Suspense fallback={<Spinner />}>
          <ReportsPage />
        </Suspense>
      </AppShell>
    )
  }

  // Default: Dashboard
  return (
    <AppShell>
      <Dashboard />
    </AppShell>
  )
}

export default App
