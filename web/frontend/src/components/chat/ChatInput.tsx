import { memo, useState, useRef, useCallback, useEffect } from 'react'
import { useChatStore } from '../../store/chatStore'

/**
 * Chat input with send button and SSE streaming.
 */
export const ChatInput = memo(function ChatInput() {
  const [input, setInput] = useState('')
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const eventSourceRef = useRef<EventSource | null>(null)
  const {
    addMessage,
    appendToLastMessage,
    setStreaming,
    setLoading,
    setConversationId,
    conversationId,
    isLoading,
    setError,
  } = useChatStore()

  // Auto-resize textarea
  useEffect(() => {
    if (textareaRef.current) {
      textareaRef.current.style.height = 'auto'
      textareaRef.current.style.height = `${Math.min(textareaRef.current.scrollHeight, 120)}px`
    }
  }, [input])

  // Cleanup EventSource on unmount
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        eventSourceRef.current.close()
      }
    }
  }, [])

  const handleSubmit = useCallback(async () => {
    const message = input.trim()
    if (!message || isLoading) return

    setInput('')
    setError(null)

    // Add user message
    addMessage({ role: 'user', content: message })

    // Add empty assistant message for streaming
    addMessage({ role: 'assistant', content: '', isStreaming: true })
    setLoading(true)

    // Close any existing connection
    if (eventSourceRef.current) {
      eventSourceRef.current.close()
    }

    // Build SSE URL
    const params = new URLSearchParams({ message })
    if (conversationId) {
      params.set('conversation_id', conversationId)
    }
    const url = `/api/chat/stream?${params.toString()}`

    const eventSource = new EventSource(url)
    eventSourceRef.current = eventSource

    eventSource.addEventListener('chunk', (e) => {
      try {
        const data = JSON.parse(e.data)
        if (data.text) {
          appendToLastMessage(data.text)
        }
      } catch (err) {
        console.error('Failed to parse chunk:', err)
      }
    })

    eventSource.addEventListener('tool_call', () => {
      // Tool calls are now handled in the message display
    })

    eventSource.addEventListener('tool_result', () => {
      // Tool result received, response will continue
    })

    eventSource.addEventListener('end', (e) => {
      try {
        const data = JSON.parse(e.data)
        if (data.conversation_id) {
          setConversationId(data.conversation_id)
        }
      } catch (err) {
        console.error('Failed to parse end:', err)
      }
      setStreaming(false)
      setLoading(false)
      eventSource.close()
      eventSourceRef.current = null
    })

    eventSource.addEventListener('error', (e) => {
      try {
        const data = JSON.parse((e as MessageEvent).data || '{}')
        setError(data.error || 'Stream error')
      } catch {
        // Connection error, not a message error
        if (eventSource.readyState === EventSource.CLOSED) {
          // Normal close
        } else {
          setError('Connection failed')
        }
      }
      setStreaming(false)
      setLoading(false)
      eventSource.close()
      eventSourceRef.current = null
    })

    eventSource.onerror = () => {
      if (eventSource.readyState === EventSource.CLOSED) {
        // Normal close after 'end' event
        setStreaming(false)
        setLoading(false)
      } else if (eventSource.readyState === EventSource.CONNECTING) {
        // Reconnecting, wait
        console.log('SSE reconnecting...')
      } else {
        setError('Connection lost')
        setStreaming(false)
        setLoading(false)
        eventSource.close()
        eventSourceRef.current = null
      }
    }
  }, [input, isLoading, conversationId, addMessage, appendToLastMessage, setStreaming, setLoading, setConversationId, setError])

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault()
        handleSubmit()
      }
    },
    [handleSubmit]
  )

  return (
    <div className="border-t border-slate-200 bg-white px-3 py-4">
      <div className="flex items-end gap-3">
        {/* Input container */}
        <div className="flex-1 relative">
          <textarea
            ref={textareaRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Ask about sales, customers, orders..."
            disabled={isLoading}
            rows={1}
            className="w-full px-4 py-3 pr-12
              bg-slate-50 border border-slate-200 rounded-2xl
              text-sm text-slate-900 placeholder-slate-400
              resize-none
              focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent focus:bg-white
              disabled:bg-slate-100 disabled:cursor-not-allowed
              min-h-[48px] max-h-[120px]
              transition-all duration-200"
          />

          {/* Character count */}
          {input.length > 100 && (
            <span className={`absolute right-3 bottom-3 text-[10px] ${
              input.length > 1800 ? 'text-red-400' : 'text-slate-400'
            }`}>
              {input.length}/2000
            </span>
          )}
        </div>

        {/* Send button */}
        <button
          onClick={handleSubmit}
          disabled={!input.trim() || isLoading}
          className="w-12 h-12 rounded-2xl
            bg-gradient-to-br from-blue-600 to-blue-700 text-white
            hover:from-blue-700 hover:to-blue-800
            disabled:from-slate-300 disabled:to-slate-300 disabled:cursor-not-allowed
            shadow-lg shadow-blue-500/25 hover:shadow-blue-500/40
            disabled:shadow-none
            transition-all duration-200
            flex items-center justify-center flex-shrink-0"
          title="Send message (Enter)"
        >
          {isLoading ? (
            <svg
              className="animate-spin h-5 w-5"
              xmlns="http://www.w3.org/2000/svg"
              fill="none"
              viewBox="0 0 24 24"
            >
              <circle
                className="opacity-25"
                cx="12"
                cy="12"
                r="10"
                stroke="currentColor"
                strokeWidth="4"
              />
              <path
                className="opacity-75"
                fill="currentColor"
                d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
              />
            </svg>
          ) : (
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="h-5 w-5"
              viewBox="0 0 20 20"
              fill="currentColor"
            >
              <path d="M10.894 2.553a1 1 0 00-1.788 0l-7 14a1 1 0 001.169 1.409l5-1.429A1 1 0 009 15.571V11a1 1 0 112 0v4.571a1 1 0 00.725.962l5 1.428a1 1 0 001.17-1.408l-7-14z" />
            </svg>
          )}
        </button>
      </div>

      {/* Helper text */}
      <div className="flex items-center justify-between mt-2 px-1">
        <p className="text-[10px] text-slate-400">
          <kbd className="px-1.5 py-0.5 bg-slate-100 rounded text-[9px] font-mono">Enter</kbd> to send
          <span className="mx-1">·</span>
          <kbd className="px-1.5 py-0.5 bg-slate-100 rounded text-[9px] font-mono">Shift+Enter</kbd> for new line
        </p>
      </div>
    </div>
  )
})
