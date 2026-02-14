import { memo, useState, useEffect } from 'react'
import { Header, Dashboard } from './components/layout'
import { ChatToggle, ChatSidebar } from './components/chat'
import { AdminUsersPage } from './components/admin'
import { useIsAdmin } from './hooks/useAuth'

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

// ─── Dashboard Shell ─────────────────────────────────────────────────────────

const DashboardShell = memo(function DashboardShell() {
  return (
    <div className="min-h-screen bg-slate-50 flex flex-col">
      <Header />
      <div className="flex-1">
        <Dashboard />
      </div>

      {/* Chat Assistant - fixed position elements outside DOM flow */}
      <ChatToggle />
      <ChatSidebar />
    </div>
  )
})

// ─── Admin Shell ─────────────────────────────────────────────────────────────

const AdminShell = memo(function AdminShell() {
  const isAdmin = useIsAdmin()

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

// ─── App Component ───────────────────────────────────────────────────────────

function App() {
  const path = useSimpleRouter()

  // Route to admin page
  if (path === '/v2/admin/users' || path === '/admin/users') {
    return <AdminShell />
  }

  // Default: Dashboard
  return <DashboardShell />
}

export default App
