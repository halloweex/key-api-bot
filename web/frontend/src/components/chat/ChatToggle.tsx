import { memo } from 'react'
import { useChatStore } from '../../store/chatStore'

// Panel icon (right side panel) with thick divider line
const PanelRightIcon = () => (
  <svg className="h-6 w-6" viewBox="0 0 24 24">
    <rect x="2" y="2" width="20" height="20" rx="2" fill="none" stroke="currentColor" strokeWidth={1.5} />
    <rect x="14" y="2" width="8" height="20" fill="currentColor" fillOpacity="0.2" />
    <line x1="14" y1="2" x2="14" y2="22" stroke="currentColor" strokeWidth={2.5} />
  </svg>
)

/**
 * Fixed position toggle button for AI chat panel.
 * Rendered outside header DOM to avoid stacking context issues.
 */
export const ChatToggle = memo(function ChatToggle() {
  const { isOpen, toggleOpen } = useChatStore()

  return (
    <button
      onClick={toggleOpen}
      className="fixed top-3 right-4 z-[100] p-1.5 rounded-lg
        transition-colors text-slate-500 hover:text-slate-700 hover:bg-slate-200"
      title={isOpen ? 'Close AI Assistant (Esc)' : 'Open AI Assistant (Ctrl+K)'}
      aria-label={isOpen ? 'Close AI Assistant' : 'Open AI Assistant'}
      aria-expanded={isOpen}
    >
      <PanelRightIcon />
    </button>
  )
})
