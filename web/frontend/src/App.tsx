import { memo, lazy, Suspense, useEffect, useState, useCallback, type ReactNode } from 'react'
import { Header, Dashboard } from './components/layout'
import { ChatSidebar } from './components/chat'
import { SidebarRail } from './components/navigation'
import { AdminUsersPage, AdminPermissionsPage } from './components/admin'
import { useAuth } from './hooks/useAuth'
import { useToast } from './components/ui/Toast'
import { useRouter, navigate } from './hooks/useRouter'
import { useNavStore } from './store/navStore'

// Lazy load pages
const TrafficPage = lazy(() => import('./components/traffic/TrafficPage'))
const ProductIntelPage = lazy(() => import('./components/products/ProductIntelPage'))
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

// ─── Sidebar Push Logic ──────────────────────────────────────────────────────
//
// Formula: push content only when there's enough room for it.
//
//   contentWidth = viewport - sidebarExpanded - chatSidebar
//   canPush = contentWidth >= MIN_CONTENT_WIDTH
//
// This naturally handles:
//   - Browser zoom (zoom ↑ → innerWidth ↓ → falls below threshold → overlay)
//   - Small screens (same effect)
//   - Large/ultrawide screens (always push)
//
// Examples (sidebarExpanded=280, chatSidebar=48, minContent=900):
//   1440px @ 100% → content = 1112px → push ✓
//   1440px @ 125% → content =  824px → overlay ✓ (effective viewport 1152px)
//   1920px @ 125% → content = 1208px → push ✓ (effective viewport 1536px)
//   1280px @ 100% → content =  952px → push ✓
//   1280px @ 110% → content =  815px → overlay ✓ (effective viewport 1163px)

const SIDEBAR_EXPANDED = 280
const CHAT_SIDEBAR = 48
const MIN_CONTENT_WIDTH = 900

function useCanPushSidebar(): boolean {
  const calc = useCallback(
    () => window.innerWidth - SIDEBAR_EXPANDED - CHAT_SIDEBAR >= MIN_CONTENT_WIDTH,
    [],
  )
  const [canPush, setCanPush] = useState(calc)

  useEffect(() => {
    const onResize = () => setCanPush(calc())
    window.addEventListener('resize', onResize)
    // Also fires on zoom in some browsers via visualViewport
    window.visualViewport?.addEventListener('resize', onResize)
    return () => {
      window.removeEventListener('resize', onResize)
      window.visualViewport?.removeEventListener('resize', onResize)
    }
  }, [calc])

  return canPush
}

// ─── Shared App Shell ─────────────────────────────────────────────────────────

const AppShell = memo(function AppShell({ children }: { children: ReactNode }) {
  useWelcomeToast()
  const sidebarOpen = useNavStore((s) => s.isOpen)
  const canPush = useCanPushSidebar()
  const pushOpen = canPush && sidebarOpen

  return (
    <div className="min-h-screen bg-slate-50 flex flex-col">
      <div className={`flex-1 flex flex-col sm:mr-12 transition-[margin-left] duration-200 ease-out ${pushOpen ? 'sm:ml-[280px]' : 'sm:ml-12'}`}>
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

  // Product Intelligence
  if (path === '/v2/products' || path === '/products') {
    return (
      <AppShell>
        <Suspense fallback={<PageSpinner />}>
          <ProductIntelPage />
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
