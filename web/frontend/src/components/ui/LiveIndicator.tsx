import { memo } from 'react'
import type { ConnectionState } from '../../hooks/useWebSocket'

interface LiveIndicatorProps {
  connectionState: ConnectionState
  lastMessageTime: Date | null
  className?: string
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
  className = '',
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

  // Format last update time
  const formatLastUpdate = () => {
    if (!lastMessageTime) return null

    const now = new Date()
    const diffSeconds = Math.floor((now.getTime() - lastMessageTime.getTime()) / 1000)

    if (diffSeconds < 60) {
      return 'just now'
    } else if (diffSeconds < 3600) {
      const mins = Math.floor(diffSeconds / 60)
      return `${mins}m ago`
    } else {
      return lastMessageTime.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
    }
  }

  const lastUpdate = formatLastUpdate()

  return (
    <div
      className={`inline-flex items-center gap-1.5 text-xs ${className}`}
      title={lastUpdate ? `Last update: ${lastUpdate}` : `Status: ${connectionState}`}
    >
      {/* Animated dot */}
      <span className="relative flex h-2 w-2">
        {showPulse && (
          <span
            className={`absolute inline-flex h-full w-full animate-ping rounded-full opacity-75 ${pulseColor}`}
          />
        )}
        <span className={`relative inline-flex h-2 w-2 rounded-full ${dotColor}`} />
      </span>

      {/* Label */}
      <span className="text-gray-600">{label}</span>

      {/* Optional: Last update time */}
      {lastUpdate && connectionState === 'connected' && (
        <span className="text-gray-400 hidden sm:inline">
          ({lastUpdate})
        </span>
      )}
    </div>
  )
})

/**
 * Compact version showing just the dot (for tight spaces).
 */
export const LiveDot = memo(function LiveDot({
  connectionState,
  className = '',
}: {
  connectionState: ConnectionState
  className?: string
}) {
  const getColor = () => {
    switch (connectionState) {
      case 'connected':
        return 'bg-green-500'
      case 'connecting':
      case 'reconnecting':
        return 'bg-yellow-500'
      default:
        return 'bg-gray-400'
    }
  }

  const isAnimated = connectionState === 'connected' || connectionState === 'reconnecting'

  return (
    <span
      className={`relative flex h-2.5 w-2.5 ${className}`}
      title={connectionState === 'connected' ? 'Real-time updates active' : `Status: ${connectionState}`}
    >
      {isAnimated && (
        <span
          className={`absolute inline-flex h-full w-full animate-ping rounded-full opacity-75 ${getColor()}`}
        />
      )}
      <span className={`relative inline-flex h-2.5 w-2.5 rounded-full ${getColor()}`} />
    </span>
  )
})

export default LiveIndicator
