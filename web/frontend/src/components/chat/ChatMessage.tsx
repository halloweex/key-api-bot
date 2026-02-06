import { memo, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import type { ChatMessage as ChatMessageType } from '../../store/chatStore'

interface ChatMessageProps {
  message: ChatMessageType
  index: number
}

/**
 * Individual chat message with markdown support and animations.
 */
export const ChatMessage = memo(function ChatMessage({ message, index }: ChatMessageProps) {
  const isUser = message.role === 'user'
  const [copied, setCopied] = useState(false)

  const handleCopy = async () => {
    await navigator.clipboard.writeText(message.content)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <div
      className={`flex ${isUser ? 'justify-end' : 'justify-start'} animate-fade-in group`}
      style={{ animationDelay: `${index * 50}ms` }}
    >
      <div className={`relative max-w-[85%] ${isUser ? 'order-1' : 'order-2'}`}>
        {/* Avatar for assistant */}
        {!isUser && (
          <div className="absolute -left-10 top-0 w-7 h-7 rounded-lg bg-gradient-to-br from-blue-500 to-purple-600 flex items-center justify-center shadow-sm">
            <svg className="w-4 h-4 text-white" viewBox="0 0 20 20" fill="currentColor">
              <path d="M9.049 2.927c.3-.921 1.603-.921 1.902 0l1.07 3.292a1 1 0 00.95.69h3.462c.969 0 1.371 1.24.588 1.81l-2.8 2.034a1 1 0 00-.364 1.118l1.07 3.292c.3.921-.755 1.688-1.54 1.118l-2.8-2.034a1 1 0 00-1.175 0l-2.8 2.034c-.784.57-1.838-.197-1.539-1.118l1.07-3.292a1 1 0 00-.364-1.118L2.98 8.72c-.783-.57-.38-1.81.588-1.81h3.461a1 1 0 00.951-.69l1.07-3.292z" />
            </svg>
          </div>
        )}

        {/* Message bubble */}
        <div
          className={`px-4 py-3 rounded-2xl shadow-sm ${
            isUser
              ? 'bg-gradient-to-br from-blue-600 to-blue-700 text-white rounded-br-md'
              : 'bg-white border border-slate-200/60 text-slate-800 rounded-bl-md'
          }`}
        >
          {/* Message content with markdown */}
          <div className={`text-sm leading-relaxed ${isUser ? '' : 'prose prose-sm prose-slate max-w-none'}`}>
            {isUser ? (
              <p className="whitespace-pre-wrap break-words">{message.content}</p>
            ) : (
              <>
                <ReactMarkdown
                  components={{
                    p: ({ children }) => <p className="mb-2 last:mb-0">{children}</p>,
                    strong: ({ children }) => <strong className="font-semibold text-slate-900">{children}</strong>,
                    ul: ({ children }) => <ul className="list-disc list-inside mb-2 space-y-1">{children}</ul>,
                    ol: ({ children }) => <ol className="list-decimal list-inside mb-2 space-y-1">{children}</ol>,
                    li: ({ children }) => <li className="text-slate-700">{children}</li>,
                    code: ({ children }) => (
                      <code className="px-1.5 py-0.5 bg-slate-100 rounded text-xs font-mono text-slate-800">
                        {children}
                      </code>
                    ),
                    h1: ({ children }) => <h1 className="text-lg font-bold mb-2 text-slate-900">{children}</h1>,
                    h2: ({ children }) => <h2 className="text-base font-bold mb-2 text-slate-900">{children}</h2>,
                    h3: ({ children }) => <h3 className="text-sm font-bold mb-1 text-slate-900">{children}</h3>,
                  }}
                >
                  {message.content}
                </ReactMarkdown>
                {message.isStreaming && (
                  <span className="inline-block w-2 h-4 bg-blue-500 animate-pulse ml-0.5 rounded-sm" />
                )}
              </>
            )}
          </div>

          {/* Tool calls visualization */}
          {message.toolCalls && message.toolCalls.length > 0 && (
            <div className="mt-3 pt-3 border-t border-slate-200/60 space-y-2">
              {message.toolCalls.map((tool, idx) => (
                <ToolCallCard key={idx} tool={tool} />
              ))}
            </div>
          )}
        </div>

        {/* Actions bar - visible on hover */}
        {!isUser && !message.isStreaming && message.content && (
          <div className="absolute -bottom-6 left-0 opacity-0 group-hover:opacity-100 transition-opacity flex items-center gap-2">
            <button
              onClick={handleCopy}
              className="text-[10px] text-slate-400 hover:text-slate-600 flex items-center gap-1 transition-colors"
            >
              {copied ? (
                <>
                  <CheckIcon className="w-3 h-3" />
                  <span>Copied</span>
                </>
              ) : (
                <>
                  <CopyIcon className="w-3 h-3" />
                  <span>Copy</span>
                </>
              )}
            </button>
            <span className="text-[10px] text-slate-300">{formatTime(message.timestamp)}</span>
          </div>
        )}
      </div>
    </div>
  )
})

/**
 * Tool call visualization card.
 */
function ToolCallCard({ tool }: { tool: { tool: string; input: Record<string, unknown>; result?: Record<string, unknown> } }) {
  const toolInfo = getToolInfo(tool.tool)

  return (
    <div className="flex items-center gap-2 px-3 py-2 bg-slate-50 rounded-lg border border-slate-100">
      <div className={`w-6 h-6 rounded-md flex items-center justify-center ${toolInfo.bgColor}`}>
        <span className="text-xs">{toolInfo.icon}</span>
      </div>
      <div className="flex-1 min-w-0">
        <div className="text-xs font-medium text-slate-700">{toolInfo.label}</div>
        {tool.input && Object.keys(tool.input).length > 0 && (
          <div className="text-[10px] text-slate-500 truncate">
            {Object.entries(tool.input).map(([k, v]) => `${k}: ${v}`).join(', ')}
          </div>
        )}
      </div>
      {tool.result && (
        <div className="w-5 h-5 rounded-full bg-green-100 flex items-center justify-center">
          <CheckIcon className="w-3 h-3 text-green-600" />
        </div>
      )}
    </div>
  )
}

function getToolInfo(toolName: string) {
  const tools: Record<string, { icon: string; label: string; bgColor: string }> = {
    get_revenue_summary: { icon: '📊', label: 'Fetching revenue data', bgColor: 'bg-blue-100' },
    get_top_products: { icon: '🏆', label: 'Finding top products', bgColor: 'bg-amber-100' },
    get_source_breakdown: { icon: '📈', label: 'Analyzing sources', bgColor: 'bg-purple-100' },
    compare_periods: { icon: '📅', label: 'Comparing periods', bgColor: 'bg-green-100' },
    get_customer_insights: { icon: '👥', label: 'Getting customer insights', bgColor: 'bg-pink-100' },
    search_buyer: { icon: '🔍', label: 'Searching customers', bgColor: 'bg-cyan-100' },
    search_order: { icon: '📦', label: 'Finding orders', bgColor: 'bg-orange-100' },
    search_product: { icon: '🛍️', label: 'Searching products', bgColor: 'bg-indigo-100' },
    get_buyer_details: { icon: '👤', label: 'Loading customer profile', bgColor: 'bg-teal-100' },
    get_order_details: { icon: '📋', label: 'Loading order details', bgColor: 'bg-rose-100' },
  }
  return tools[toolName] || { icon: '⚡', label: toolName.replace(/_/g, ' '), bgColor: 'bg-slate-100' }
}

function formatTime(date: Date): string {
  return new Intl.DateTimeFormat('uk-UA', {
    hour: '2-digit',
    minute: '2-digit',
  }).format(date)
}

function CopyIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" />
    </svg>
  )
}

function CheckIcon({ className }: { className?: string }) {
  return (
    <svg className={className} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
      <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
    </svg>
  )
}
