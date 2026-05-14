import { memo } from 'react'
import type { ConnectionState } from '../hooks/useWebSocket'

interface LiveIndicatorProps {
  connectionState: ConnectionState
  lastMessageTime: Date | null
}

/**
 * Small indicator showing WebSocket connection status.
 * - Green dot + "Live" when connected
 * - Yellow dot + "Reconnecting" when reconnecting
 * - Gray dot + "Offline" when disconnected
 */
export const LiveIndicator = memo(function LiveIndicator({
  connectionState,
  lastMessageTime,
}: LiveIndicatorProps) {
  const getStatusConfig = () => {
    switch (connectionState) {
      case 'connected':
        return {
          dotColor: 'bg-green-500',
          pulseColor: 'bg-green-400',
          label: 'Live',
          showPulse: true,
        }
      case 'connecting':
      case 'reconnecting':
        return {
          dotColor: 'bg-yellow-500',
          pulseColor: 'bg-yellow-400',
          label: 'Reconnecting',
          showPulse: true,
        }
      case 'disconnected':
      default:
        return {
          dotColor: 'bg-gray-400',
          pulseColor: 'bg-gray-300',
          label: 'Offline',
          showPulse: false,
        }
    }
  }

  const { dotColor, pulseColor, label, showPulse } = getStatusConfig()

  const formatLastUpdate = () => {
    if (!lastMessageTime) return null
    const now = new Date()
    const diffSeconds = Math.floor((now.getTime() - lastMessageTime.getTime()) / 1000)
    if (diffSeconds < 60) return 'just now'
    if (diffSeconds < 3600) {
      const mins = Math.floor(diffSeconds / 60)
      return `${mins}m ago`
    }
    return lastMessageTime.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
  }

  const lastUpdate = formatLastUpdate()

  return (
    <div
      className="inline-flex items-center gap-1.5 text-xs"
      title={lastUpdate ? `Last update: ${lastUpdate}` : `Status: ${connectionState}`}
    >
      <span className="relative flex h-2 w-2">
        {showPulse && (
          <span
            className={`absolute inline-flex h-full w-full animate-ping rounded-full opacity-75 ${pulseColor}`}
          />
        )}
        <span className={`relative inline-flex h-2 w-2 rounded-full ${dotColor}`} />
      </span>

      <span className="text-gray-600">{label}</span>

      {lastUpdate && connectionState === 'connected' && (
        <span className="text-gray-400 hidden sm:inline">({lastUpdate})</span>
      )}
    </div>
  )
})
