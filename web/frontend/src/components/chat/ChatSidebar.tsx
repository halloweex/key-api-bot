import { memo, useEffect, useCallback } from 'react'
import { useChatStore } from '../../store/chatStore'
import { ChatHeader } from './ChatHeader'
import { ChatMessages } from './ChatMessages'
import { ChatInput } from './ChatInput'

/**
 * Sliding sidebar panel for AI chat assistant.
 */
export const ChatSidebar = memo(function ChatSidebar() {
  const { isOpen, setOpen } = useChatStore()

  // Handle keyboard shortcuts
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      // Escape to close
      if (e.key === 'Escape' && isOpen) {
        setOpen(false)
      }

      // Ctrl+K or Cmd+K to toggle
      if ((e.ctrlKey || e.metaKey) && e.key === 'k') {
        e.preventDefault()
        setOpen(!isOpen)
      }
    },
    [isOpen, setOpen]
  )

  useEffect(() => {
    document.addEventListener('keydown', handleKeyDown)
    return () => document.removeEventListener('keydown', handleKeyDown)
  }, [handleKeyDown])

  return (
    <>
      {/* Optional backdrop for mobile - click to close */}
      <div
        className={`fixed inset-0 bg-black/30 backdrop-blur-sm z-[60] transition-all duration-300 sm:hidden
          ${isOpen ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
        onClick={() => setOpen(false)}
        aria-hidden="true"
      />

      {/* Sidebar Panel - starts below toggle button */}
      <aside
        className={`fixed top-14 right-0 bottom-0 w-full sm:w-[400px] z-[60]
          bg-gradient-to-b from-white to-slate-50
          shadow-[-20px_0_60px_-15px_rgba(0,0,0,0.15)]
          flex flex-col
          transform transition-transform duration-300 ease-out
          ${isOpen ? 'translate-x-0' : 'translate-x-full'}`}
        role="complementary"
        aria-label="AI Assistant"
        aria-hidden={!isOpen}
      >
        {/* Decorative top border */}
        <div className="h-1 bg-gradient-to-r from-blue-500 via-purple-500 to-blue-500" />

        <ChatHeader />
        <ChatMessages />
        <ChatInput />
      </aside>
    </>
  )
})
