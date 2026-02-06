import { create } from 'zustand'

export interface ChatMessage {
  id: string
  role: 'user' | 'assistant'
  content: string
  timestamp: Date
  isStreaming?: boolean
  toolCalls?: Array<{
    tool: string
    input: Record<string, unknown>
    result?: Record<string, unknown>
  }>
}

export interface SearchResult {
  type: 'buyer' | 'order' | 'product'
  id: number
  title: string
  subtitle?: string
  highlight?: string
}

interface ChatState {
  isOpen: boolean
  messages: ChatMessage[]
  conversationId: string | null
  isLoading: boolean
  error: string | null
  searchResults: SearchResult[]

  // Actions
  setOpen: (open: boolean) => void
  toggleOpen: () => void
  addMessage: (message: Omit<ChatMessage, 'id' | 'timestamp'>) => void
  updateLastMessage: (content: string) => void
  appendToLastMessage: (text: string) => void
  setStreaming: (isStreaming: boolean) => void
  setLoading: (loading: boolean) => void
  setError: (error: string | null) => void
  setConversationId: (id: string | null) => void
  setSearchResults: (results: SearchResult[]) => void
  clearMessages: () => void
  clearSearch: () => void
}

export const useChatStore = create<ChatState>((set) => ({
  isOpen: false,
  messages: [],
  conversationId: null,
  isLoading: false,
  error: null,
  searchResults: [],

  setOpen: (open) => set({ isOpen: open }),

  toggleOpen: () => set((state) => ({ isOpen: !state.isOpen })),

  addMessage: (message) => {
    const newMessage: ChatMessage = {
      ...message,
      id: `msg_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`,
      timestamp: new Date(),
    }
    set((state) => ({
      messages: [...state.messages, newMessage],
      error: null,
    }))
  },

  updateLastMessage: (content) => {
    set((state) => {
      const messages = [...state.messages]
      if (messages.length > 0) {
        messages[messages.length - 1] = {
          ...messages[messages.length - 1],
          content,
        }
      }
      return { messages }
    })
  },

  appendToLastMessage: (text) => {
    set((state) => {
      const messages = [...state.messages]
      if (messages.length > 0) {
        messages[messages.length - 1] = {
          ...messages[messages.length - 1],
          content: messages[messages.length - 1].content + text,
        }
      }
      return { messages }
    })
  },

  setStreaming: (isStreaming) => {
    set((state) => {
      const messages = [...state.messages]
      if (messages.length > 0) {
        messages[messages.length - 1] = {
          ...messages[messages.length - 1],
          isStreaming,
        }
      }
      return { messages }
    })
  },

  setLoading: (loading) => set({ isLoading: loading }),

  setError: (error) => set({ error }),

  setConversationId: (id) => set({ conversationId: id }),

  setSearchResults: (results) => set({ searchResults: results }),

  clearMessages: () => set({ messages: [], conversationId: null, error: null }),

  clearSearch: () => set({ searchResults: [] }),
}))
