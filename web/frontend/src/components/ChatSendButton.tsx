import { memo } from 'react'
import { Loader2, Send } from 'lucide-react'

interface ChatSendButtonProps {
  onClick: () => void
  disabled?: boolean
  ariaLabel: string
  /** Shows the in-flight spinner instead of the send glyph. */
  loading?: boolean
}

/**
 * Square gradient send button used by the chat composer.
 * Visual lives entirely here — consumers pass only behavioural intent.
 */
export const ChatSendButton = memo(function ChatSendButton({
  onClick,
  disabled = false,
  ariaLabel,
  loading = false,
}: ChatSendButtonProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      aria-label={ariaLabel}
      title={ariaLabel}
      className="w-12 h-12 rounded-2xl bg-gradient-to-br from-blue-600 to-blue-700 text-white
                 hover:from-blue-700 hover:to-blue-800
                 disabled:from-slate-300 disabled:to-slate-300 disabled:cursor-not-allowed
                 shadow-lg shadow-blue-500/25 hover:shadow-blue-500/40 disabled:shadow-none
                 transition-all duration-200 flex items-center justify-center flex-shrink-0"
    >
      {loading ? (
        <Loader2 className="animate-spin h-5 w-5" aria-hidden />
      ) : (
        <Send className="h-5 w-5" aria-hidden />
      )}
    </button>
  )
})
