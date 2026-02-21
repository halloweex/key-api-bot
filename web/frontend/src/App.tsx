import { memo, lazy, Suspense, useEffect, type ReactNode } from 'react'
import { Header, Dashboard } from './components/layout'
import { ChatSidebar } from './components/chat'
import { SidebarRail } from './components/navigation'
import { AdminUsersPage, AdminPermissionsPage } from './components/admin'
import { useAuth } from './hooks/useAuth'
import { useToast } from './components/ui/Toast'
import { useRouter, navigate } from './hooks/useRouter'

// Lazy load pages
const TrafficPage = lazy(() => import('./components/traffic/TrafficPage'))
const InventoryPage = lazy(() => import('./components/inventory/InventoryPage'))
const ReportsPage = lazy(() => import('./components/reports/ReportsPage'))

// ─── Welcome Toast Hook ──────────────────────────────────────────────────────

function useWelcomeToast() {
  const { addToast } = useToast()

  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const welcomeName = params.get('welcome')

    if (welcomeName) {
      addToast({
        type: 'success',
        title: `Welcome, ${decodeURIComponent(welcomeName)}!`,
        duration: 4000,
      })

      const url = new URL(window.location.href)
      url.searchParams.delete('welcome')
      window.history.replaceState({}, '', url.pathname)
    }
  }, [addToast])
}

// ─── Shared App Shell ─────────────────────────────────────────────────────────

const AppShell = memo(function AppShell({ children }: { children: ReactNode }) {
  useWelcomeToast()

  return (
    <div className="min-h-screen bg-slate-50 flex flex-col">
      <div className="flex-1 flex flex-col sm:ml-12 sm:mr-12">
        <Header />
        {children}
      </div>
      <SidebarRail />
      <ChatSidebar />
    </div>
  )
})

// ─── Page Loading Spinner ─────────────────────────────────────────────────────

const PageSpinner = () => (
  <div className="flex-1 flex items-center justify-center">
    <div className="w-8 h-8 border-4 border-purple-500 border-t-transparent rounded-full animate-spin" />
  </div>
)

// ─── Admin Guard ──────────────────────────────────────────────────────────────

const AdminGuard = memo(function AdminGuard({ children }: { children: ReactNode }) {
  const { user, isLoading } = useAuth()
  const isAdmin = user?.role === 'admin'

  if (isLoading) {
    return (
      <div className="min-h-screen bg-slate-50 flex items-center justify-center">
        <div className="w-8 h-8 border-4 border-purple-500 border-t-transparent rounded-full animate-spin" />
      </div>
    )
  }

  if (!isAdmin) {
    navigate('/v2')
    return null
  }

  return (
    <div className="min-h-screen bg-slate-50">
      {children}
    </div>
  )
})

// ─── App Component ───────────────────────────────────────────────────────────

function App() {
  const path = useRouter()

  // Admin pages (no AppShell - they have their own layout)
  if (path === '/v2/admin/users' || path === '/admin/users') {
    return <AdminGuard><AdminUsersPage /></AdminGuard>
  }

  if (path === '/v2/admin/permissions' || path === '/admin/permissions') {
    return <AdminGuard><AdminPermissionsPage /></AdminGuard>
  }

  // Traffic Analytics
  if (path === '/v2/traffic' || path === '/traffic') {
    return (
      <AppShell>
        <Suspense fallback={<PageSpinner />}>
          <TrafficPage />
        </Suspense>
      </AppShell>
    )
  }

  // Inventory
  if (path === '/v2/inventory' || path === '/inventory') {
    return (
      <AppShell>
        <Suspense fallback={<PageSpinner />}>
          <InventoryPage />
        </Suspense>
      </AppShell>
    )
  }

  // Reports
  if (path === '/v2/reports' || path === '/reports') {
    return (
      <AppShell>
        <Suspense fallback={<PageSpinner />}>
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
