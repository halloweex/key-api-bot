import { describe, it, expect, beforeEach, vi } from 'vitest'
import { chunkReloader, isMissingChunk, lazyChunk } from '../lazyChunk'

/**
 * The whole point of the helper is what it does *not* do, so most of these
 * are about restraint: reload for a missing chunk, once, and never for an
 * error the module raised itself.
 */

const FLAG = 'ks:chunk-reloaded'

/** `lazy()` defers the factory until React renders, so call it directly. */
function load(component: unknown): Promise<unknown> {
  return (component as { _payload: { _result: () => Promise<unknown> } })
    ._payload._result()
}

beforeEach(() => {
  vi.unstubAllGlobals()
  sessionStorage.clear()
  vi.restoreAllMocks()
})

describe('isMissingChunk', () => {
  it('recognises how each engine words it', () => {
    expect(isMissingChunk(new TypeError(
      'Failed to fetch dynamically imported module: https://x/assets/a.js',
    ))).toBe(true)
    expect(isMissingChunk(new TypeError(
      'error loading dynamically imported module',
    ))).toBe(true)
    expect(isMissingChunk(new TypeError(
      'Importing a module script failed.',
    ))).toBe(true)
  })

  it('does not recognise a module that threw while evaluating', () => {
    // The case a reload must never be spent on: the chunk arrived and the code
    // in it is broken. Reloading would hide a real bug behind a refresh.
    expect(isMissingChunk(new ReferenceError('x is not defined'))).toBe(false)
    expect(isMissingChunk(new Error('Cannot read properties of undefined')))
      .toBe(false)
  })

  it('survives being handed something that is not an Error', () => {
    expect(isMissingChunk(undefined)).toBe(false)
    expect(isMissingChunk(null)).toBe(false)
    expect(isMissingChunk('failed to fetch')).toBe(true)
  })
})

describe('lazyChunk', () => {
  it('reloads once when the chunk is missing, and not twice', async () => {
    const reload = vi.spyOn(chunkReloader, 'reload').mockImplementation(() => {})
    const missing = () => Promise.reject(
      new TypeError('Failed to fetch dynamically imported module: /a.js'),
    )

    await expect(load(lazyChunk(missing))).rejects.toThrow()
    expect(reload).toHaveBeenCalledTimes(1)
    expect(sessionStorage.getItem(FLAG)).toBe('1')

    // A second miss in the same tab is a chunk that is genuinely gone, not a
    // stale document. One error screen beats an endless refresh.
    await expect(load(lazyChunk(missing))).rejects.toThrow()
    expect(reload).toHaveBeenCalledTimes(1)
  })

  it('does not reload when the module itself threw', async () => {
    const reload = vi.spyOn(chunkReloader, 'reload').mockImplementation(() => {})
    const broken = () => Promise.reject(new ReferenceError('boom'))

    await expect(load(lazyChunk(broken))).rejects.toThrow('boom')
    expect(reload).not.toHaveBeenCalled()
    expect(sessionStorage.getItem(FLAG)).toBeNull()
  })

  it('passes the module through untouched when it loads', async () => {
    const component = (() => null) as never
    const loaded = await load(lazyChunk(() => Promise.resolve({ default: component })))
    expect(loaded).toEqual({ default: component })
  })

  it('gives the next deploy its own attempt', async () => {
    // One recovery must not spend the tab's only reload for the rest of the
    // session — two deploys in an afternoon is exactly how this was found.
    const reload = vi.spyOn(chunkReloader, 'reload').mockImplementation(() => {})
    const missing = () => Promise.reject(
      new TypeError('Failed to fetch dynamically imported module: /a.js'),
    )

    await expect(load(lazyChunk(missing))).rejects.toThrow()
    expect(reload).toHaveBeenCalledTimes(1)

    // …the reload lands, a chunk loads fine, and the flag is spent.
    await load(lazyChunk(() => Promise.resolve({ default: (() => null) as never })))
    expect(sessionStorage.getItem(FLAG)).toBeNull()

    await expect(load(lazyChunk(missing))).rejects.toThrow()
    expect(reload).toHaveBeenCalledTimes(2)
  })

  it('refuses to reload at all when storage is unavailable', async () => {
    // No storage, no loop protection — and without protection an automatic
    // reload is the more dangerous failure of the two.
    const reload = vi.spyOn(chunkReloader, 'reload').mockImplementation(() => {})
    // Replacing the global rather than spying on `Storage.prototype`: jsdom's
    // `sessionStorage` does not dispatch through the prototype, so the spy was
    // installed, never called, and the test passed for the wrong reason until
    // it was actually run.
    vi.stubGlobal('sessionStorage', {
      getItem() { throw new DOMException('denied') },
      setItem() { throw new DOMException('denied') },
      removeItem() { throw new DOMException('denied') },
      clear() {},
    })

    await expect(load(lazyChunk(() => Promise.reject(
      new TypeError('Failed to fetch dynamically imported module: /a.js'),
    )))).rejects.toThrow()
    expect(reload).not.toHaveBeenCalled()
  })
})
