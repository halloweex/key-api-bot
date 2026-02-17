import { useSyncExternalStore } from 'react'

const NAVIGATE_EVENT = 'app:navigate'

/** Navigate to a path using pushState (no full page reload). */
export function navigate(to: string) {
  if (to === window.location.pathname) return
  window.history.pushState({}, '', to)
  window.dispatchEvent(new CustomEvent(NAVIGATE_EVENT))
}

function subscribe(callback: () => void) {
  window.addEventListener('popstate', callback)
  window.addEventListener(NAVIGATE_EVENT, callback)
  return () => {
    window.removeEventListener('popstate', callback)
    window.removeEventListener(NAVIGATE_EVENT, callback)
  }
}

function getSnapshot() {
  return window.location.pathname
}

/** Reactive hook that returns the current pathname. */
export function useRouter() {
  return useSyncExternalStore(subscribe, getSnapshot)
}
