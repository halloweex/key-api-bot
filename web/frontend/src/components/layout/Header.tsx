import { FilterBar } from '../filters'
import { useNavStore } from '../../store/navStore'
import { useChatStore } from '../../store/chatStore'

// Panel icons matching sidebar style
const PanelLeftIcon = () => (
  <svg className="h-5 w-5" viewBox="0 0 24 24">
    <rect x="2" y="2" width="20" height="20" rx="5" fill="none" stroke="currentColor" strokeWidth={1.5} />
    <rect x="2" y="2" width="8" height="20" rx="5" fill="currentColor" fillOpacity="0.2" />
    <line x1="10" y1="4" x2="10" y2="20" stroke="currentColor" strokeWidth={2} strokeLinecap="round" />
  </svg>
)

const PanelRightIcon = () => (
  <svg className="h-5 w-5" viewBox="0 0 24 24">
    <rect x="2" y="2" width="20" height="20" rx="5" fill="none" stroke="currentColor" strokeWidth={1.5} />
    <rect x="14" y="2" width="8" height="20" rx="5" fill="currentColor" fillOpacity="0.2" />
    <line x1="14" y1="4" x2="14" y2="20" stroke="currentColor" strokeWidth={2} strokeLinecap="round" />
  </svg>
)

export function Header() {
  const { isOpen: isNavOpen, setOpen: setNavOpen } = useNavStore()
  const { isOpen: isChatOpen, setOpen: setChatOpen } = useChatStore()

  return (
    <header className="sticky top-0 z-50 bg-white/90 backdrop-blur-xl border-b border-slate-200 py-2 px-3 shadow-sm">
      <div className="max-w-[1800px] flex items-center gap-2">
        {/* Mobile nav toggle - left */}
        <button
          onClick={() => setNavOpen(true)}
          className={`sm:hidden p-1.5 rounded-lg text-slate-500 hover:bg-slate-100 hover:text-slate-700 flex-shrink-0
            ${isNavOpen ? 'opacity-0 pointer-events-none' : ''}`}
          aria-label="Open menu"
        >
          <PanelLeftIcon />
        </button>

        {/* Filters */}
        <div className="flex-1 min-w-0">
          <FilterBar />
        </div>

        {/* Mobile chat toggle - right */}
        <button
          onClick={() => setChatOpen(true)}
          className={`sm:hidden p-1.5 rounded-lg text-slate-500 hover:bg-slate-100 hover:text-slate-700 flex-shrink-0
            ${isChatOpen ? 'opacity-0 pointer-events-none' : ''}`}
          aria-label="Open AI assistant"
        >
          <PanelRightIcon />
        </button>
      </div>
    </header>
  )
}
