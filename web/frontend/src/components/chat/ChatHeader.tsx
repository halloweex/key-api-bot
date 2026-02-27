import { memo } from 'react'
import { Sparkles, Trash2 } from 'lucide-react'
import { useChatStore } from '../../store/chatStore'

/**
 * Chat sidebar header with gradient and status.
 */
export const ChatHeader = memo(function ChatHeader() {
  const { clearMessages, messages } = useChatStore()

  return (
    <div className="flex items-center justify-between px-3 py-4 bg-gradient-to-r from-slate-50 via-white to-blue-50 border-b border-slate-200/60">
      <div className="flex items-center gap-3">
        {/* AI Avatar */}
        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center shadow-lg shadow-blue-500/20">
          <Sparkles className="w-5 h-5 text-white" />
        </div>

        <div>
          <h2 className="text-sm font-semibold text-slate-900">AI Assistant</h2>
          <p className="text-xs text-slate-500">Sales analytics</p>
        </div>
      </div>

      {/* Actions */}
      <div className="flex items-center gap-1">
        {/* Message count badge */}
        {messages.length > 0 && (
          <span className="px-2 py-0.5 bg-slate-100 text-slate-500 text-[10px] font-medium rounded-full mr-1">
            {messages.length} msg
          </span>
        )}

        {/* Clear conversation button */}
        {messages.length > 0 && (
          <button
            onClick={clearMessages}
            className="p-2 text-slate-400 hover:text-red-500 hover:bg-red-50 rounded-lg transition-all duration-200"
            title="Clear conversation"
          >
            <Trash2 className="h-4 w-4" />
          </button>
        )}
      </div>
    </div>
  )
})
