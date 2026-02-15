import { FilterBar } from '../filters'
import { useNavStore } from '../../store/navStore'
import { useChatStore } from '../../store/chatStore'

// Menu icon for mobile nav toggle
const MenuIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M4 6h16M4 12h16M4 18h16" />
  </svg>
)

// AI chat icon for mobile
const ChatIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z" />
  </svg>
)

export function Header() {
  const { setOpen: setNavOpen } = useNavStore()
  const { setOpen: setChatOpen } = useChatStore()

  return (
    <header className="sticky top-0 z-50 bg-white/90 backdrop-blur-xl border-b border-slate-200 py-2 px-3 shadow-sm">
      <div className="max-w-[1800px] flex items-center gap-2">
        {/* Mobile menu button - left sidebar */}
        <button
          onClick={() => setNavOpen(true)}
          className="sm:hidden p-2 -ml-1 rounded-lg hover:bg-slate-100 text-slate-600"
          aria-label="Open navigation menu"
        >
          <MenuIcon />
        </button>

        {/* Filters */}
        <div className="flex-1 min-w-0">
          <FilterBar />
        </div>

        {/* Mobile chat button - right sidebar */}
        <button
          onClick={() => setChatOpen(true)}
          className="sm:hidden p-2 -mr-1 rounded-lg hover:bg-slate-100 text-slate-600"
          aria-label="Open AI assistant"
        >
          <ChatIcon />
        </button>
      </div>
    </header>
  )
}
