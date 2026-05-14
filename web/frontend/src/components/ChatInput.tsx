import { memo, useState, useRef, useCallback, useEffect } from 'react'
import { Loader2, Send } from 'lucide-react'
import { useChatStore } from '../store/chatStore'
import { Textarea } from './Textarea'
import { ChatSendButton } from './ChatSendButton'

/**
 * Chat input with send button and SSE streaming.
 */
export const ChatInput = memo(function ChatInput() {
  const [input, setInput] = useState('')
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
        <div className="flex-1 relative">
          <Textarea
            size="md"
            fullWidth
            autoResize
            maxHeight={120}
            value={input}
            onChange={setInput}
            onKeyDown={handleKeyDown}
            placeholder="Ask about sales, customers, orders..."
            disabled={isLoading}
            rows={1}
          />

          {input.length > 100 && (
            <span className={`absolute right-3 bottom-3 text-[10px] ${
              input.length > 1800 ? 'text-red-400' : 'text-slate-400'
            }`}>
              {input.length}/2000
            </span>
          )}
        </div>

        <ChatSendButton
          onClick={handleSubmit}
          disabled={!input.trim() || isLoading}
          ariaLabel="Send message (Enter)"
          icon={isLoading ? <Loader2 className="animate-spin h-5 w-5" /> : <Send className="h-5 w-5" />}
        />
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
