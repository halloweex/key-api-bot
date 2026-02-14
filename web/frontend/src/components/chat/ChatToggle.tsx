import { memo } from 'react'
import { useChatStore } from '../../store/chatStore'

/**
 * Fixed position toggle button for AI chat panel.
 * Rendered outside header DOM to avoid stacking context issues.
 */
export const ChatToggle = memo(function ChatToggle() {
  const { isOpen, toggleOpen } = useChatStore()

  return (
    <button
      onClick={toggleOpen}
      className={`fixed top-3 right-4 z-[100]
        flex items-center gap-2 px-3 py-2 rounded-lg
        transition-all duration-300 ease-out
        shadow-sm hover:shadow
        ${isOpen
          ? 'bg-blue-100 text-blue-700 border border-blue-200 hover:bg-blue-200'
          : 'bg-white/90 backdrop-blur-sm text-slate-600 hover:text-blue-600 border border-slate-200 hover:border-slate-300 hover:bg-white'
        }`}
      title={isOpen ? 'Close AI Assistant (Esc)' : 'Open AI Assistant (Ctrl+K)'}
      aria-label={isOpen ? 'Close AI Assistant' : 'Open AI Assistant'}
      aria-expanded={isOpen}
    >
      {/* Panel icon */}
      <svg className="h-5 w-5" viewBox="0 0 24 24">
        <rect x="3" y="3" width="18" height="18" rx="2" fill="none" stroke="currentColor" strokeWidth={2} />
        <rect x="15" y="3" width="6" height="18" fill="currentColor" fillOpacity="0.3" />
        <path d="M15 3v18" stroke="currentColor" strokeWidth={2} />
      </svg>
      <span className="text-sm font-medium hidden sm:block">Toggle AI</span>
    </button>
  )
})
