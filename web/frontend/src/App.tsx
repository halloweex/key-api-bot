import { memo, useState, useEffect } from 'react'
import { Header, Dashboard } from './components/layout'
import { ChatToggle, ChatSidebar } from './components/chat'
import { AdminUsersPage, AdminPermissionsPage } from './components/admin'
import { UserProfileDropdown } from './components/ui/UserProfileDropdown'
import { useAuth } from './hooks/useAuth'
import { useToast } from './components/ui/Toast'

// ─── Simple Router Hook ──────────────────────────────────────────────────────

function useSimpleRouter() {
  const [path, setPath] = useState(window.location.pathname)

  useEffect(() => {
    const handlePopState = () => setPath(window.location.pathname)
    window.addEventListener('popstate', handlePopState)
    return () => window.removeEventListener('popstate', handlePopState)
  }, [])

  return path
}

// ─── Welcome Toast Hook ──────────────────────────────────────────────────────

function useWelcomeToast() {
  const { addToast } = useToast()

  useEffect(() => {
    const params = new URLSearchParams(window.location.search)
    const welcomeName = params.get('welcome')

    if (welcomeName) {
      // Show welcome toast
      addToast({
        type: 'success',
        title: `Welcome, ${decodeURIComponent(welcomeName)}!`,
        duration: 4000,
      })

      // Clean up URL (remove ?welcome=... parameter)
      const url = new URL(window.location.href)
      url.searchParams.delete('welcome')
      window.history.replaceState({}, '', url.pathname)
    }
  }, [addToast])
}

// ─── Dashboard Shell ─────────────────────────────────────────────────────────

const DashboardShell = memo(function DashboardShell() {
  useWelcomeToast()

  return (
    <div className="min-h-screen bg-slate-50 flex flex-col">
      <Header />
      <div className="flex-1">
        <Dashboard />
      </div>

      {/* Fixed position elements outside DOM flow */}
      <ChatToggle />
      <ChatSidebar />

      {/* User profile - fixed bottom-left */}
      <div className="fixed bottom-4 left-4 z-[100]">
        <UserProfileDropdown />
      </div>
    </div>
  )
})

// ─── Admin Shell ─────────────────────────────────────────────────────────────

const AdminUsersShell = memo(function AdminUsersShell() {
  const { user, isLoading } = useAuth()
  const isAdmin = user?.role === 'admin'

  // Wait for auth to load before checking
  if (isLoading) {
    return (
      <div className="min-h-screen bg-slate-50 flex items-center justify-center">
        <div className="w-8 h-8 border-4 border-purple-500 border-t-transparent rounded-full animate-spin" />
      </div>
    )
  }

  // Redirect non-admins back to dashboard
  if (!isAdmin) {
    window.location.href = '/v2'
    return null
  }

  return (
    <div className="min-h-screen bg-slate-50">
      <AdminUsersPage />
    </div>
  )
})

const AdminPermissionsShell = memo(function AdminPermissionsShell() {
  const { user, isLoading } = useAuth()
  const isAdmin = user?.role === 'admin'

  // Wait for auth to load before checking
  if (isLoading) {
    return (
      <div className="min-h-screen bg-slate-50 flex items-center justify-center">
        <div className="w-8 h-8 border-4 border-purple-500 border-t-transparent rounded-full animate-spin" />
      </div>
    )
  }

  // Redirect non-admins back to dashboard
  if (!isAdmin) {
    window.location.href = '/v2'
    return null
  }

  return (
    <div className="min-h-screen bg-slate-50">
      <AdminPermissionsPage />
    </div>
  )
})

// ─── App Component ───────────────────────────────────────────────────────────

function App() {
  const path = useSimpleRouter()

  // Route to admin pages
  if (path === '/v2/admin/users' || path === '/admin/users') {
    return <AdminUsersShell />
  }

  if (path === '/v2/admin/permissions' || path === '/admin/permissions') {
    return <AdminPermissionsShell />
  }

  // Default: Dashboard
  return <DashboardShell />
}

export default App
