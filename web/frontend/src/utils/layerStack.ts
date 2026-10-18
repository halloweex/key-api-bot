/**
 * What is open on top of the page — a bottom sheet, a modal dialog — in the
 * order it opened.
 *
 * "Back" means the top one first. Telegram's BackButton is one button for
 * the whole Mini App, and a person pressing it with a condition's sheet open
 * means "close this", not "leave the step under it". So every layer that can
 * be dismissed registers its own close here while it is open, and the button
 * asks `closeTopLayer()` before it does anything else.
 *
 * In memory, for this tab. A layer that unmounts takes itself off.
 */
import { useEffect, useRef, useSyncExternalStore } from 'react'

const stack: Array<{ close: () => void }> = []
const listeners = new Set<() => void>()

function changed() {
  for (const listener of listeners) listener()
}

/** Register a layer as open; the returned function takes it off again. */
export function pushLayer(close: () => void): () => void {
  const layer = { close }
  stack.push(layer)
  changed()
  return () => {
    const i = stack.indexOf(layer)
    if (i >= 0) {
      stack.splice(i, 1)
      changed()
    }
  }
}

/** Close the layer on top, if any; true when one was closed. */
export function closeTopLayer(): boolean {
  const top = stack[stack.length - 1]
  if (!top) return false
  top.close()
  return true
}

function subscribe(listener: () => void) {
  listeners.add(listener)
  return () => { listeners.delete(listener) }
}

/** Is anything open on top of the page? */
export function useAnyLayerOpen(): boolean {
  return useSyncExternalStore(subscribe, () => stack.length > 0, () => false)
}

/**
 * Hold this component's place on the stack while `open`. `close` may change
 * every render; the latest one is called.
 */
export function useLayer(open: boolean, close: () => void): void {
  const latest = useRef(close)
  useEffect(() => { latest.current = close }, [close])
  useEffect(() => (open ? pushLayer(() => latest.current()) : undefined), [open])
}
