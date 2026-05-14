import '@testing-library/jest-dom/vitest'
import { afterEach } from 'vitest'
import { cleanup } from '@testing-library/react'

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
