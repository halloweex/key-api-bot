import { useEffect, useSyncExternalStore, type RefObject } from 'react'
import { NAVIGATE_EVENT } from './useRouter'
import { hashTarget } from '../utils/deepLink'

// ─── Landing on a section: /marketing#promotions ─────────────────────────────
//
// The page is a single-page app and the browser's own jump to an anchor does
// not happen: the section is not in the document when the HTML loads, and the
// page's own `navigate` pushes the hash without a `hashchange`. So the section
// asks whether the hash names it, and scrolls itself into view.

function subscribe(callback: () => void) {
  window.addEventListener('hashchange', callback)
  window.addEventListener('popstate', callback)
  window.addEventListener(NAVIGATE_EVENT, callback)
  return () => {
    window.removeEventListener('hashchange', callback)
    window.removeEventListener('popstate', callback)
    window.removeEventListener(NAVIGATE_EVENT, callback)
  }
}

/** Whether the location's hash names `anchor`, Telegram's launch parameters
 *  after it or not (`utils/deepLink.hashTarget`). */
export function useHashTarget(anchor: string): boolean {
  return useSyncExternalStore(
    subscribe,
    () => hashTarget(window.location.hash) === anchor,
    () => false,
  )
}

/** How long the section is held in view while the page above it loads. */
const SETTLE_MS = 2500
/** The reader's own hand on the page ends the hold at once. */
const STOP_EVENTS = ['wheel', 'touchstart', 'keydown', 'pointerdown'] as const

/**
 * Scroll `ref` to the top of the view when `active` turns true, and keep it
 * there while the page settles. On /marketing the monthly report above the
 * calendar arrives after it and grows by a screen, which would push the
 * section out of the view it was just scrolled into; so every resize of the
 * page aligns it again, for `SETTLE_MS` or until the reader scrolls, taps or
 * types. The sticky header's height is the section's own `scroll-margin`.
 */
export function useScrollIntoViewWhenSettled(
  ref: RefObject<HTMLElement | null>,
  active: boolean,
): void {
  useEffect(() => {
    if (!active) return
    const el = ref.current
    if (!el) return
    let done = false
    // jsdom has no scrollIntoView; a browser always does.
    const align = () => { if (!done) el.scrollIntoView?.({ block: 'start' }) }
    // After the frame that lays the section out, with the form it opens.
    const frame = requestAnimationFrame(align)
    const observer = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(align)
    observer?.observe(el.closest('main') ?? document.body)
    // Called only once `timer` is set: by the timer, an event or the cleanup.
    function stop() {
      done = true
      cancelAnimationFrame(frame)
      observer?.disconnect()
      clearTimeout(timer)
      STOP_EVENTS.forEach((name) => window.removeEventListener(name, stop))
    }
    const timer = setTimeout(stop, SETTLE_MS)
    STOP_EVENTS.forEach((name) => window.addEventListener(name, stop, { passive: true }))
    return stop
  }, [ref, active])
}
