import { memo } from 'react'
import { PanelRight } from 'lucide-react'
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
      className="fixed top-3 right-4 z-[100] p-1.5 rounded-lg
        transition-colors text-slate-500 hover:text-slate-700 hover:bg-slate-200"
      title={isOpen ? 'Close AI Assistant (Esc)' : 'Open AI Assistant (Ctrl+K)'}
      aria-label={isOpen ? 'Close AI Assistant' : 'Open AI Assistant'}
      aria-expanded={isOpen}
    >
      <PanelRight className="h-6 w-6" />
    </button>
  )
})
