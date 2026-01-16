import { memo } from 'react'
import { Header, Dashboard } from './components/layout'

// ─── App Shell ───────────────────────────────────────────────────────────────

const AppShell = memo(function AppShell() {
  return (
    <div className="min-h-screen bg-slate-900 flex flex-col">
      <Header />
      <div className="flex-1">
        <Dashboard />
      </div>
    </div>
  )
})

// ─── App Component ───────────────────────────────────────────────────────────

function App() {
  return <AppShell />
}

export default App
