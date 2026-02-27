import { memo, useEffect, useCallback } from 'react'
import { PanelRight } from 'lucide-react'
import { useChatStore } from '../../store/chatStore'
import { ChatMessages } from './ChatMessages'
import { ChatInput } from './ChatInput'

/**
 * Collapsible sidebar panel for AI chat assistant.
 * Shows a rail when collapsed, full panel when expanded.
 */
export const ChatSidebar = memo(function ChatSidebar() {
  const { isOpen, setOpen } = useChatStore()

  const toggleOpen = useCallback(() => {
    setOpen(!isOpen)
  }, [isOpen, setOpen])

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
      {/* Mobile backdrop */}
      <div
        className={`fixed inset-0 bg-black/30 z-[54] sm:hidden
          transition-opacity duration-200
          ${isOpen ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
        onClick={() => setOpen(false)}
        aria-hidden="true"
      />

      {/* Mobile toggle button - top right */}
      <button
        onClick={() => setOpen(true)}
        className={`fixed top-3 right-3 z-[53] sm:hidden
          w-8 h-8 rounded-lg bg-white border border-slate-200 shadow-sm
          flex items-center justify-center text-slate-500
          hover:bg-slate-50 active:bg-slate-100
          ${isOpen ? 'opacity-0 pointer-events-none' : 'opacity-100'}`}
        aria-label="Open AI assistant"
      >
        <PanelRight className="h-5 w-5" />
      </button>

      <aside
        className={`fixed top-0 right-0 bottom-0 z-[55]
          bg-slate-50
          border-l border-slate-200
          transition-transform duration-200 ease-out
          ${isOpen
            ? 'translate-x-0 w-[280px]'
            : 'translate-x-full sm:translate-x-0 sm:w-12 sm:cursor-pointer sm:hover:bg-slate-100'
          }`}
        role="complementary"
        aria-label="AI Assistant"
        onClick={isOpen ? undefined : toggleOpen}
      >
        {/* Header area - always visible */}
        <div className="h-14 flex items-center border-b border-slate-200">
          {/* Collapsed: centered panel icon */}
          <div
            className={`absolute left-0 right-0 flex justify-center
              ${isOpen ? 'opacity-0 pointer-events-none' : 'opacity-100'}`}
          >
            <div className="p-1.5 text-slate-500">
              <PanelRight className="h-5 w-5" />
            </div>
          </div>

          {/* Expanded: header with toggle */}
          <div
            className={`flex items-center justify-between w-full px-3
              ${isOpen ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
          >
            <div className="min-w-0">
              <h2 className="text-sm font-semibold text-slate-900">AI Assistant</h2>
              <p className="text-[10px] text-slate-500">Sales analytics</p>
            </div>
            <button
              onClick={toggleOpen}
              className="p-1.5 rounded-lg hover:bg-slate-200 text-slate-500 hover:text-slate-700"
              title="Collapse panel (Esc)"
              aria-label="Collapse AI panel"
            >
              <PanelRight className="h-5 w-5" />
            </button>
          </div>
        </div>

        {/* Expanded content */}
        <div
          className={`flex flex-col h-[calc(100%-3.5rem)]
            ${isOpen ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
        >
          <ChatMessages />
          <ChatInput />
        </div>
      </aside>
    </>
  )
})
