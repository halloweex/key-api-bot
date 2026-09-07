import { Suspense } from 'react'
import { lazyChunk } from './utils/lazyChunk'
import { Dashboard } from './components/Dashboard'
import { AppShell } from './components/AppShell'
import { AdminGuard, PermissionGuard } from './components/RouteGuard'
import { Spinner } from './components/Spinner'
import { useRouter } from './hooks/useRouter'

// Lazy load pages
const AdminUsersPage = lazyChunk(() => import('./components/AdminUsersPage').then(m => ({ default: m.AdminUsersPage })))
const AdminPermissionsPage = lazyChunk(() => import('./components/AdminPermissionsPage').then(m => ({ default: m.AdminPermissionsPage })))
const TrafficPage = lazyChunk(() => import('./components/TrafficPage'))
const ProductIntelPage = lazyChunk(() => import('./components/ProductIntelPage'))
const InventoryPage = lazyChunk(() => import('./components/InventoryPage'))
const ReportsPage = lazyChunk(() => import('./components/ReportsPage'))
const MarketingPage = lazyChunk(() => import('./components/MarketingPage'))
const MarginPage = lazyChunk(() => import('./components/MarginPage'))
const SmsCampaignsPage = lazyChunk(() => import('./components/SmsCampaignsPage'))

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
        <PermissionGuard feature="traffic">
          <Suspense fallback={<Spinner />}>
            <TrafficPage />
          </Suspense>
        </PermissionGuard>
      </AppShell>
    )
  }

  // Product Intelligence
  if (path === '/v2/products' || path === '/products') {
    return (
      <AppShell>
        <PermissionGuard feature="products">
          <Suspense fallback={<Spinner />}>
            <ProductIntelPage />
          </Suspense>
        </PermissionGuard>
      </AppShell>
    )
  }

  // Inventory
  if (path === '/v2/inventory' || path === '/inventory') {
    return (
      <AppShell>
        <PermissionGuard feature="inventory">
          <Suspense fallback={<Spinner />}>
            <InventoryPage />
          </Suspense>
        </PermissionGuard>
      </AppShell>
    )
  }

  // Marketing
  if (path === '/v2/marketing' || path === '/marketing') {
    return (
      <AppShell>
        <PermissionGuard feature="marketing">
          <Suspense fallback={<Spinner />}>
            <MarketingPage />
          </Suspense>
        </PermissionGuard>
      </AppShell>
    )
  }

  // Margin analysis. Cost and profit, so only the admin role holds it by
  // default — but it is a permission now rather than a role check, which is
  // what lets one person be given it without being made an admin.
  if (path === '/v2/margin' || path === '/margin') {
    return (
      <AppShell>
        <PermissionGuard feature="margin">
          <Suspense fallback={<Spinner />}>
            <MarginPage />
          </Suspense>
        </PermissionGuard>
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
        <PermissionGuard feature="reports">
          <Suspense fallback={<Spinner />}>
            <ReportsPage />
          </Suspense>
        </PermissionGuard>
      </AppShell>
    )
  }

  // Default: Dashboard. Guarded like every other tab — an account granted
  // /traffic alone lands here first and is sent on to the page it can open.
  return (
    <AppShell>
      <PermissionGuard feature="dashboard">
        <Dashboard />
      </PermissionGuard>
    </AppShell>
  )
}

export default App
