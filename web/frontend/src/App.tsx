import { memo, useState, useEffect } from 'react'
import { Header, Dashboard } from './components/layout'
import { ChatToggle, ChatSidebar } from './components/chat'
import { AdminUsersPage } from './components/admin'
import { useIsAdmin } from './hooks/useAuth'
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
