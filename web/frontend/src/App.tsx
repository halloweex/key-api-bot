import { memo } from 'react'
import { Header, Dashboard } from './components/layout'
import { ChatToggle, ChatSidebar } from './components/chat'

// ─── App Shell ───────────────────────────────────────────────────────────────

const AppShell = memo(function AppShell() {
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

// ─── App Component ───────────────────────────────────────────────────────────

function App() {
  return <AppShell />
}

export default App
