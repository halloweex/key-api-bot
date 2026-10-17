import '@testing-library/jest-dom/vitest'
import { afterEach } from 'vitest'
import { cleanup } from '@testing-library/react'

// Node 22+ has a `localStorage` global of its own, and without
// `--localstorage-file` it is an object with no methods — which shadows
// jsdom's. `src/lib/i18n.ts` reads it while being imported, so any test that
// reaches the formatters fails before it starts. A map stands in, only when
// the real one is missing its methods.
if (typeof globalThis.localStorage?.getItem !== 'function') {
  const store = new Map<string, string>()
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => void store.set(k, String(v)),
      removeItem: (k: string) => void store.delete(k),
      clear: () => store.clear(),
    },
  })
}

// i18next mock — every component uses useTranslation; return the key untouched
// so we don't depend on translation files in tests.
import { vi } from 'vitest'

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) =>
      opts && typeof opts.count === 'number' ? `${key}{${opts.count}}` : key,
    i18n: { language: 'en', changeLanguage: () => Promise.resolve() },
  }),
  initReactI18next: { type: '3rdParty', init: () => {} },
}))

// Lottie pulls in a big web worker / canvas — stub the player.
vi.mock('lottie-react', () => ({
  default: () => null,
}))

afterEach(() => {
  cleanup()
})
