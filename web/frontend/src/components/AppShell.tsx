import { memo, useCallback, useEffect, useState, type ReactNode } from 'react'
import { Header } from './Header'
import { ChatSidebar } from './ChatSidebar'
import { SidebarRail } from './SidebarRail'
import { useToast } from './Toast'
import { useNavStore } from '../store/navStore'

// ─── AppShell ────────────────────────────────────────────────────────────────
//
// The chrome every in-app page lives inside: header, nav rail, chat sidebar,
// and the content column that shifts when the rail is pinned open. Pages are
// passed as children and own nothing of this layout.

// ─── Welcome Toast ───────────────────────────────────────────────────────────

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

// ─── Component ───────────────────────────────────────────────────────────────

export const AppShell = memo(function AppShell({ children }: { children: ReactNode }) {
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
