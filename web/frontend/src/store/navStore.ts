import { create } from 'zustand'
import { useChatStore } from './chatStore'

interface NavStore {
  isOpen: boolean
  setOpen: (open: boolean) => void
  toggleOpen: () => void
}

export const useNavStore = create<NavStore>((set) => ({
  isOpen: false,

  setOpen: (open) => {
    // Close chat sidebar when opening nav
    if (open) {
      useChatStore.getState().setOpen(false)
    }
    set({ isOpen: open })
  },

  toggleOpen: () => {
    const current = useNavStore.getState().isOpen
    useNavStore.getState().setOpen(!current)
  },
}))
