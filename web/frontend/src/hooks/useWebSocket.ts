import { useEffect, useRef, useCallback, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'

// WebSocket event types from backend
export type WebSocketEvent =
  | 'orders_synced'
  | 'products_synced'
  | 'inventory_updated'
  | 'goal_progress'
  | 'milestone_reached'
  | 'sync_status'
  | 'connected'
  | 'pong'

export interface WebSocketMessage {
  event: WebSocketEvent
  data: Record<string, unknown>
  timestamp: string
}

export type ConnectionState = 'connecting' | 'connected' | 'disconnected' | 'reconnecting'

interface UseWebSocketOptions {
  enabled?: boolean
  room?: string
  onMessage?: (message: WebSocketMessage) => void
  onMilestone?: (data: { amount: number; type: string }) => void
}

const MAX_RECONNECT_ATTEMPTS = 5
const BASE_RECONNECT_DELAY = 1000 // 1 second
const MAX_RECONNECT_DELAY = 30000 // 30 seconds
const PING_INTERVAL = 30000 // 30 seconds

export function useWebSocket(options: UseWebSocketOptions = {}) {
  const {
    enabled = true,
    room = 'dashboard',
    onMessage,
    onMilestone,
  } = options

  const ws = useRef<WebSocket | null>(null)
  const queryClient = useQueryClient()
  const reconnectAttempts = useRef(0)
  const reconnectTimeout = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)
  const pingInterval = useRef<ReturnType<typeof setInterval> | undefined>(undefined)

  const [connectionState, setConnectionState] = useState<ConnectionState>('disconnected')
  const [lastMessageTime, setLastMessageTime] = useState<Date | null>(null)

  // Clean up function
  const cleanup = useCallback(() => {
    if (reconnectTimeout.current) {
      clearTimeout(reconnectTimeout.current)
    }
    if (pingInterval.current) {
      clearInterval(pingInterval.current)
    }
    if (ws.current) {
      ws.current.onopen = null
      ws.current.onmessage = null
      ws.current.onerror = null
      ws.current.onclose = null
      if (ws.current.readyState === WebSocket.OPEN) {
        ws.current.close()
      }
      ws.current = null
    }
  }, [])

  // Handle incoming messages
  const handleMessage = useCallback((event: MessageEvent) => {
    try {
      const message: WebSocketMessage = JSON.parse(event.data)
      setLastMessageTime(new Date())

      // Call custom handler if provided
      if (onMessage) {
        onMessage(message)
      }

      // Handle specific events
      switch (message.event) {
        case 'orders_synced':
          // Invalidate summary and revenue queries to trigger refetch
          queryClient.invalidateQueries({ queryKey: ['summary'] })
          queryClient.invalidateQueries({ queryKey: ['revenueTrend'] })
          queryClient.invalidateQueries({ queryKey: ['salesBySource'] })
          break

        case 'products_synced':
          queryClient.invalidateQueries({ queryKey: ['topProducts'] })
          queryClient.invalidateQueries({ queryKey: ['productPerformance'] })
          break

        case 'inventory_updated':
          queryClient.invalidateQueries({ queryKey: ['stockSummary'] })
          queryClient.invalidateQueries({ queryKey: ['inventoryTrend'] })
          queryClient.invalidateQueries({ queryKey: ['inventoryAnalysis'] })
          break

        case 'goal_progress':
          queryClient.invalidateQueries({ queryKey: ['goals'] })
          queryClient.invalidateQueries({ queryKey: ['smartGoals'] })
          break

        case 'milestone_reached':
          // Dispatch custom event for celebration UI
          if (onMilestone && message.data) {
            onMilestone(message.data as { amount: number; type: string })
          }
          // Also dispatch DOM event for global listeners
          window.dispatchEvent(
            new CustomEvent('milestone', { detail: message.data })
          )
          break

        case 'connected':
          // Server confirmed connection
          break

        case 'pong':
          // Keep-alive response received
          break
      }
    } catch (e) {
      console.warn('Failed to parse WebSocket message:', e)
    }
  }, [queryClient, onMessage, onMilestone])

  // Connect to WebSocket
  const connect = useCallback(() => {
    if (!enabled) return

    // Don't connect if already connected or connecting
    if (ws.current?.readyState === WebSocket.OPEN ||
        ws.current?.readyState === WebSocket.CONNECTING) {
      return
    }

    setConnectionState('connecting')

    // Determine WebSocket URL
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    const host = window.location.host
    const url = `${protocol}//${host}/ws/${room}`

    try {
      ws.current = new WebSocket(url)

      ws.current.onopen = () => {
        console.log(`WebSocket connected to ${room}`)
        setConnectionState('connected')
        reconnectAttempts.current = 0

        // Start ping interval for keep-alive
        pingInterval.current = setInterval(() => {
          if (ws.current?.readyState === WebSocket.OPEN) {
            ws.current.send('ping')
          }
        }, PING_INTERVAL)
      }

      ws.current.onmessage = handleMessage

      ws.current.onerror = (error) => {
        console.warn('WebSocket error:', error)
      }

      ws.current.onclose = (event) => {
        console.log(`WebSocket closed: ${event.code} ${event.reason}`)
        setConnectionState('disconnected')

        // Clear ping interval
        if (pingInterval.current) {
          clearInterval(pingInterval.current)
        }

        // Attempt to reconnect with exponential backoff
        if (enabled && reconnectAttempts.current < MAX_RECONNECT_ATTEMPTS) {
          setConnectionState('reconnecting')
          const delay = Math.min(
            BASE_RECONNECT_DELAY * Math.pow(2, reconnectAttempts.current),
            MAX_RECONNECT_DELAY
          )
          console.log(`Reconnecting in ${delay}ms (attempt ${reconnectAttempts.current + 1})`)
          reconnectAttempts.current++
          reconnectTimeout.current = setTimeout(connect, delay)
        }
      }
    } catch (e) {
      console.error('Failed to create WebSocket:', e)
      setConnectionState('disconnected')
    }
  }, [enabled, room, handleMessage])

  // Disconnect from WebSocket
  const disconnect = useCallback(() => {
    cleanup()
    setConnectionState('disconnected')
    reconnectAttempts.current = 0
  }, [cleanup])

  // Send a message
  const send = useCallback((data: string | object) => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      const message = typeof data === 'string' ? data : JSON.stringify(data)
      ws.current.send(message)
    }
  }, [])

  // Connect on mount, disconnect on unmount
  useEffect(() => {
    if (enabled) {
      connect()
    }

    return () => {
      cleanup()
    }
  }, [enabled, connect, cleanup])

  return {
    connectionState,
    isConnected: connectionState === 'connected',
    lastMessageTime,
    connect,
    disconnect,
    send,
  }
}

export default useWebSocket
