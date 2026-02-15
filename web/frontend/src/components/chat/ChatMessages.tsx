import { memo, useEffect, useRef } from 'react'
import { useChatStore } from '../../store/chatStore'
import { ChatMessage } from './ChatMessage'

/**
 * Scrollable message list with improved empty state.
 */
export const ChatMessages = memo(function ChatMessages() {
  const { messages, isLoading } = useChatStore()
  const messagesEndRef = useRef<HTMLDivElement>(null)

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  if (messages.length === 0) {
    return <EmptyState />
  }

  return (
    <div className="flex-1 overflow-y-auto px-3 py-4 space-y-6 scrollbar-none [&::-webkit-scrollbar]:hidden [-ms-overflow-style:none]">
      {messages.map((message, index) => (
        <ChatMessage key={message.id} message={message} index={index} />
      ))}

      {/* Typing indicator */}
      {isLoading && messages[messages.length - 1]?.role !== 'assistant' && (
        <TypingIndicator />
      )}

      <div ref={messagesEndRef} className="h-4" />
    </div>
  )
})

/**
 * Empty state with welcome message and suggestions.
 */
const EmptyState = memo(function EmptyState() {
  const { addMessage, setLoading, setConversationId, appendToLastMessage, setStreaming } = useChatStore()

  const handleSuggestion = async (text: string) => {
    addMessage({ role: 'user', content: text })
    addMessage({ role: 'assistant', content: '', isStreaming: true })
    setLoading(true)

    const params = new URLSearchParams({ message: text })
    const eventSource = new EventSource(`/api/chat/stream?${params.toString()}`)

    eventSource.addEventListener('chunk', (e) => {
      try {
        const data = JSON.parse(e.data)
        if (data.text) appendToLastMessage(data.text)
      } catch {}
    })

    eventSource.addEventListener('end', (e) => {
      try {
        const data = JSON.parse(e.data)
        if (data.conversation_id) setConversationId(data.conversation_id)
      } catch {}
      setStreaming(false)
      setLoading(false)
      eventSource.close()
    })

    eventSource.onerror = () => {
      setStreaming(false)
      setLoading(false)
      eventSource.close()
    }
  }

  return (
    <div className="flex-1 flex flex-col items-center justify-center p-6 text-center">
      {/* AI Avatar */}
      <div className="relative mb-6">
        <div className="w-20 h-20 rounded-2xl bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center shadow-lg">
          <svg className="w-10 h-10 text-white" viewBox="0 0 20 20" fill="currentColor">
            <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
          </svg>
        </div>
        <div className="absolute -bottom-1 -right-1 w-6 h-6 bg-green-500 rounded-full border-2 border-white flex items-center justify-center">
          <span className="text-white text-xs">✓</span>
        </div>
      </div>

      {/* Welcome text */}
      <h3 className="text-xl font-semibold text-slate-900 mb-2">
        AI Sales Assistant
      </h3>
      <p className="text-sm text-slate-500 mb-8 max-w-[280px]">
        Ask me anything about your sales, customers, or products. I can analyze data and find insights for you.
      </p>

      {/* Capability cards */}
      <div className="w-full max-w-[320px] space-y-2 mb-6">
        <CapabilityCard
          icon="📊"
          title="Sales Analytics"
          description="Revenue, trends, comparisons"
        />
        <CapabilityCard
          icon="🔍"
          title="Smart Search"
          description="Find customers, orders, products"
        />
        <CapabilityCard
          icon="💡"
          title="Business Insights"
          description="Top performers, customer behavior"
        />
      </div>

      {/* Quick suggestions */}
      <div className="w-full max-w-[320px]">
        <p className="text-xs text-slate-400 uppercase tracking-wide mb-3 font-medium">
          Try asking
        </p>
        <div className="flex flex-wrap gap-2 justify-center">
          {SUGGESTIONS.map((suggestion) => (
            <button
              key={suggestion.text}
              onClick={() => handleSuggestion(suggestion.text)}
              className="px-3 py-2 bg-white border border-slate-200 rounded-xl
                text-sm text-slate-700 hover:bg-slate-50 hover:border-slate-300
                hover:shadow-sm transition-all duration-200 flex items-center gap-2"
            >
              <span>{suggestion.icon}</span>
              <span>{suggestion.text}</span>
            </button>
          ))}
        </div>
      </div>
    </div>
  )
})

function CapabilityCard({ icon, title, description }: { icon: string; title: string; description: string }) {
  return (
    <div className="flex items-center gap-3 px-4 py-3 bg-slate-50 rounded-xl text-left">
      <div className="w-10 h-10 rounded-lg bg-white shadow-sm flex items-center justify-center text-lg">
        {icon}
      </div>
      <div>
        <div className="text-sm font-medium text-slate-800">{title}</div>
        <div className="text-xs text-slate-500">{description}</div>
      </div>
    </div>
  )
}

function TypingIndicator() {
  return (
    <div className="flex justify-start animate-fade-in">
      <div className="flex items-center gap-2">
        <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center">
          <svg className="w-4 h-4 text-white" viewBox="0 0 20 20" fill="currentColor">
            <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
          </svg>
        </div>
        <div className="bg-white border border-slate-200/60 px-4 py-3 rounded-2xl rounded-bl-md shadow-sm">
          <div className="flex gap-1.5">
            <span className="w-2 h-2 bg-slate-400 rounded-full animate-bounce" />
            <span className="w-2 h-2 bg-slate-400 rounded-full animate-bounce [animation-delay:0.15s]" />
            <span className="w-2 h-2 bg-slate-400 rounded-full animate-bounce [animation-delay:0.3s]" />
          </div>
        </div>
      </div>
    </div>
  )
}

const SUGGESTIONS = [
  { text: "How are sales today?", icon: "📊" },
  { text: "Top products this week", icon: "🏆" },
  { text: "Compare to last week", icon: "📈" },
]
