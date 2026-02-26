/**
 * i18next initialization and configuration.
 *
 * Must be imported before any component renders (side-effect import in main.tsx).
 */
import i18n from 'i18next'
import { initReactI18next } from 'react-i18next'

import en from '../locales/en.json'
import uk from '../locales/uk.json'
import ru from '../locales/ru.json'

export const SUPPORTED_LANGUAGES = ['en', 'uk', 'ru'] as const
export type SupportedLanguage = typeof SUPPORTED_LANGUAGES[number]

export const LANGUAGE_LABELS: Record<SupportedLanguage, string> = {
  en: 'English',
  uk: 'Українська',
  ru: 'Русский',
}

export const LANGUAGE_FLAGS: Record<SupportedLanguage, string> = {
  en: '\uD83C\uDDEC\uD83C\uDDE7',
  uk: '\uD83C\uDDFA\uD83C\uDDE6',
  ru: '\uD83C\uDDF7\uD83C\uDDFA',
}

/** Maps i18n language code to Intl locale for number/date formatting */
export const LANGUAGE_LOCALES: Record<SupportedLanguage, string> = {
  en: 'en-US',
  uk: 'uk-UA',
  ru: 'ru-RU',
}

const STORAGE_KEY = 'ks_language'

function getSavedLanguage(): SupportedLanguage {
  const saved = localStorage.getItem(STORAGE_KEY)
  if (saved && SUPPORTED_LANGUAGES.includes(saved as SupportedLanguage)) {
    return saved as SupportedLanguage
  }
  return 'en'
}

i18n
  .use(initReactI18next)
  .init({
    resources: {
      en: { translation: en },
      uk: { translation: uk },
      ru: { translation: ru },
    },
    lng: getSavedLanguage(),
    fallbackLng: 'en',
    supportedLngs: [...SUPPORTED_LANGUAGES],
    interpolation: {
      escapeValue: false, // React already escapes
    },
  })

/** Persist language to localStorage on change */
i18n.on('languageChanged', (lng) => {
  localStorage.setItem(STORAGE_KEY, lng)
})

export default i18n
