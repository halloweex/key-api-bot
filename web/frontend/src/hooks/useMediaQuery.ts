import { useEffect, useState } from 'react'

/**
 * Does the viewport match `query`, and keep saying so as it changes.
 *
 * Reads `matchMedia` once on mount so the first render is already right — a
 * phone that first renders the desktop popover and then swaps it for a sheet
 * flashes the wrong control under the thumb. Where there is no `matchMedia`
 * (tests, old embedded webviews) the answer is false.
 */
export function useMediaQuery(query: string): boolean {
  const get = () =>
    typeof window !== 'undefined' && typeof window.matchMedia === 'function'
      ? window.matchMedia(query).matches
      : false
  const [matches, setMatches] = useState(get)

  useEffect(() => {
    if (typeof window === 'undefined' || typeof window.matchMedia !== 'function') return
    const list = window.matchMedia(query)
    const update = () => setMatches(list.matches)
    update()
    list.addEventListener?.('change', update)
    return () => list.removeEventListener?.('change', update)
  }, [query])

  return matches
}

/**
 * Below Tailwind's `sm` (640px): one column, bottom sheets instead of
 * popovers, and a sticky footer under the thumb.
 */
export function useIsPhone(): boolean {
  return useMediaQuery('(max-width: 639px)')
}
