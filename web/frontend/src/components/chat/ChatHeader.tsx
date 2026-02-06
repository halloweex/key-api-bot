import { memo } from 'react'
import { useChatStore } from '../../store/chatStore'

/**
 * Chat sidebar header with gradient and status.
 */
export const ChatHeader = memo(function ChatHeader() {
  const { clearMessages, messages } = useChatStore()

  return (
    <div className="flex items-center justify-between px-4 py-4 bg-gradient-to-r from-slate-50 via-white to-blue-50 border-b border-slate-200/60">
      <div className="flex items-center gap-3">
        {/* AI Avatar */}
        <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center shadow-lg shadow-blue-500/20">
          <svg className="w-5 h-5 text-white" viewBox="0 0 20 20" fill="currentColor">
            <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
          </svg>
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
            <svg
              className="h-4 w-4"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16"
              />
            </svg>
          </button>
        )}
      </div>
    </div>
  )
})
