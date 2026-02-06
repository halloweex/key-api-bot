/**
 * Chat API client with SSE streaming support.
 */

import { ApiError, NetworkError } from './client'

const API_BASE = '/api'

// ─── Types ───────────────────────────────────────────────────────────────────

export interface ChatRequest {
  message: string
  conversation_id?: string
  context?: Record<string, unknown>
}

export interface ChatResponse {
  conversation_id: string
  content: string
  tokens_used?: number
  error?: boolean
}

export interface SearchResult {
  type: 'buyers' | 'orders' | 'products'
  id: number
  [key: string]: unknown
}

export interface SearchResponse {
  query: string
  buyers: SearchResult[]
  orders: SearchResult[]
  products: SearchResult[]
  total_hits: number
}

export interface StreamEvent {
  type: 'chunk' | 'tool_call' | 'tool_result' | 'end' | 'error'
  text?: string
  tool?: string
  input?: Record<string, unknown>
  result?: Record<string, unknown>
  conversation_id?: string
  tokens_used?: number
  error?: string
}

// ─── Chat API ────────────────────────────────────────────────────────────────

export const chatApi = {
  /**
   * Send a chat message (non-streaming).
   */
  async sendMessage(request: ChatRequest): Promise<ChatResponse> {
    const response = await fetch(`${API_BASE}/chat`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      credentials: 'include',
      body: JSON.stringify(request),
    })

    if (!response.ok) {
      throw ApiError.fromResponse(response)
    }

    return response.json()
  },

  /**
   * Stream a chat response using SSE.
   */
  streamMessage(
    message: string,
    conversationId?: string,
    onEvent?: (event: StreamEvent) => void,
    signal?: AbortSignal
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      const params = new URLSearchParams({
        message,
        ...(conversationId && { conversation_id: conversationId }),
      })

      const url = `${API_BASE}/chat/stream?${params.toString()}`

      // Note: withCredentials not needed when using Vite proxy (same origin)
      const eventSource = new EventSource(url)

      const cleanup = () => {
        eventSource.close()
      }

      // Handle abort signal
      if (signal) {
        signal.addEventListener('abort', () => {
          cleanup()
          reject(new DOMException('Aborted', 'AbortError'))
        })
      }

      eventSource.addEventListener('chunk', (e) => {
        try {
          const data = JSON.parse(e.data)
          onEvent?.({ type: 'chunk', text: data.text })
        } catch (err) {
          console.error('Failed to parse chunk:', err)
        }
      })

      eventSource.addEventListener('tool_call', (e) => {
        try {
          const data = JSON.parse(e.data)
          onEvent?.({ type: 'tool_call', tool: data.tool, input: data.input })
        } catch (err) {
          console.error('Failed to parse tool_call:', err)
        }
      })

      eventSource.addEventListener('tool_result', (e) => {
        try {
          const data = JSON.parse(e.data)
          onEvent?.({ type: 'tool_result', tool: data.tool, result: data.result })
        } catch (err) {
          console.error('Failed to parse tool_result:', err)
        }
      })

      eventSource.addEventListener('end', (e) => {
        try {
          const data = JSON.parse(e.data)
          onEvent?.({
            type: 'end',
            conversation_id: data.conversation_id,
            tokens_used: data.tokens_used,
          })
        } catch (err) {
          console.error('Failed to parse end:', err)
        }
        cleanup()
        resolve()
      })

      // Handle connection errors
      eventSource.onerror = () => {
        // CLOSED means normal end, CONNECTING means temporary issue
        if (eventSource.readyState === EventSource.CLOSED) {
          cleanup()
          resolve()
        } else if (eventSource.readyState === EventSource.CONNECTING) {
          // Retry is happening, don't error yet
          console.log('SSE reconnecting...')
        } else {
          cleanup()
          onEvent?.({ type: 'error', error: 'Connection error' })
          reject(new NetworkError('Stream connection failed'))
        }
      }
    })
  },

  /**
   * Get chat service status.
   */
  async getStatus(): Promise<{ available: boolean; active_conversations: number }> {
    const response = await fetch(`${API_BASE}/chat/status`, { credentials: 'include' })
    if (!response.ok) {
      throw ApiError.fromResponse(response)
    }
    return response.json()
  },
}

// ─── Search API ──────────────────────────────────────────────────────────────

export const searchApi = {
  /**
   * Universal search across buyers, orders, and products.
   */
  async search(
    query: string,
    type: 'all' | 'buyers' | 'orders' | 'products' = 'all',
    limit = 10
  ): Promise<SearchResponse> {
    const params = new URLSearchParams({
      q: query,
      type,
      limit: String(limit),
    })

    const response = await fetch(`${API_BASE}/search?${params.toString()}`, {
      credentials: 'include',
    })

    if (!response.ok) {
      throw ApiError.fromResponse(response)
    }

    return response.json()
  },

  /**
   * Get buyer details.
   */
  async getBuyerDetails(buyerId: number): Promise<Record<string, unknown>> {
    const response = await fetch(`${API_BASE}/buyers/${buyerId}`, {
      credentials: 'include',
    })
    if (!response.ok) {
      throw ApiError.fromResponse(response)
    }
    return response.json()
  },

  /**
   * Get order details.
   */
  async getOrderDetails(orderId: number): Promise<Record<string, unknown>> {
    const response = await fetch(`${API_BASE}/orders/${orderId}`, {
      credentials: 'include',
    })
    if (!response.ok) {
      throw ApiError.fromResponse(response)
    }
    return response.json()
  },
}
