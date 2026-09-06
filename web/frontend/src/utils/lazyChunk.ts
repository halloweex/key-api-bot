/**
 * `React.lazy`, but a chunk that went missing under the tab reloads it.
 *
 * THE FAILURE THIS EXISTS FOR
 *
 * Every deploy rebuilds the frontend, and Vite names each chunk by the hash of
 * its content — so `ReportsPage-DRz6VXil.js` becomes `ReportsPage-CzCOmilC.js`
 * and the old name stops existing. A tab that loaded `index.html` before the
 * deploy is still holding the *old* chunk map. The moment it navigates to a
 * page it has not visited yet, the dynamic import 404s, the promise rejects,
 * and the error boundary paints "Something went wrong" over a dashboard that
 * is in perfect health.
 *
 * Seen in production on 2026-09-06 after three deploys inside two hours. One
 * 404 in the server log, forty-six 200s around it, and nothing wrong with the
 * server at all.
 *
 * WHY A RELOAD IS THE RIGHT ANSWER HERE AND USUALLY IS NOT
 *
 * Reloading on error is normally a way to turn one failure into a loop. It is
 * right in this one case because the failure is *known to be fixed by it*: the
 * chunk is missing because this document is stale, and a reload replaces the
 * document. The two things that keep it from becoming a loop are both here —
 * the reload happens at most once per tab, and it only happens for a failure
 * that looks like a missing module rather than for any error the module itself
 * throws while evaluating.
 *
 * IT IS THE SECOND BELT, NOT THE FIRST
 *
 * The first is `Cache-Control: no-cache` on the HTML shell, so a reload
 * actually fetches the new chunk map instead of the cached old one. Without
 * that, this helper reloads once and lands on the same stale document — the
 * guard stops the loop and the user still sees the error. Fixed together in
 * `nginx/nginx.conf`; neither half is sufficient alone.
 */
import { lazy } from 'react'
import type { ComponentType, LazyExoticComponent } from 'react'

const RELOAD_FLAG = 'ks:chunk-reloaded'

/**
 * Is this the browser saying it could not *fetch* the module?
 *
 * Narrow on purpose. A module that loads and then throws while evaluating is a
 * real bug, and reloading would hide one failure behind another. The three
 * engines word it differently and none of them gives a typed error, so the
 * message is all there is to go on.
 */
export function isMissingChunk(error: unknown): boolean {
  const message = String(
    (error as { message?: unknown } | null)?.message ?? error ?? '',
  ).toLowerCase()
  return (
    // Chromium
    message.includes('failed to fetch dynamically imported module') ||
    // Firefox
    message.includes('error loading dynamically imported module') ||
    // WebKit
    message.includes('importing a module script failed') ||
    // Vite's own preload helper, which fetches the chunk itself
    message.includes('unable to preload css') ||
    message.includes('failed to fetch')
  )
}

/** Storage can throw outright — private windows, blocked site data. */
function readFlag(): boolean {
  try {
    return sessionStorage.getItem(RELOAD_FLAG) === '1'
  } catch {
    // No storage means no loop protection, and no protection means no reload:
    // an error screen is recoverable by hand, an infinite refresh is not.
    return true
  }
}

function writeFlag(value: '1' | null): void {
  try {
    if (value === null) sessionStorage.removeItem(RELOAD_FLAG)
    else sessionStorage.setItem(RELOAD_FLAG, value)
  } catch {
    /* see readFlag */
  }
}

/**
 * The seam the tests drive. `location.reload` cannot be called for real in a
 * test environment, and stubbing the global is worse than naming the door.
 */
export const chunkReloader = {
  reload: () => {
    if (typeof window !== 'undefined') window.location.reload()
  },
}

/**
 * Wrap a dynamic import so a stale chunk map heals itself.
 *
 * Used for every `lazy()` in the app —
 * `tests/unit/test_lazy_chunks_are_wrapped.py` fails if a bare one appears,
 * because the one page nobody wrapped is exactly the page somebody opens
 * after a deploy.
 */
// React's own constraint on `lazy` is `ComponentType<any>`, and anything
// narrower here does not satisfy it — `ComponentType<never>` was tried and
// every component with props failed to type-check.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function lazyChunk<T extends ComponentType<any>>(
  factory: () => Promise<{ default: T }>,
): LazyExoticComponent<T> {
  return lazy(() =>
    factory().then(
      (loaded) => {
        // A chunk arrived, so this document's map is good: let a *later*
        // deploy earn its own reload. Without this, one recovery would spend
        // the tab's only attempt for the rest of the session.
        writeFlag(null)
        return loaded
      },
      (error: unknown) => {
        if (isMissingChunk(error) && !readFlag()) {
          writeFlag('1')
          chunkReloader.reload()
        }
        // Rethrown either way. If the reload is coming, this loses the race
        // and nobody sees it; if it is not, the error boundary is the right
        // place for a failure this helper could not fix.
        throw error
      },
    ),
  )
}
