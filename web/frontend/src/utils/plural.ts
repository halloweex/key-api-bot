/**
 * Plurals without plural keys.
 *
 * The three dictionaries must carry the same key set
 * (`tests/unit/test_frontend_locales.py`), and Russian has four plural
 * categories where English has two. So a counted word is ONE key holding its
 * forms separated by `|` — "order|orders", «заказ|заказа|заказов» — and
 * `plural` picks the form with `Intl.PluralRules`.
 */
import i18n, { LANGUAGE_LOCALES, type SupportedLanguage } from '../lib/i18n'

export type T = (key: string, opts?: Record<string, unknown>) => string

function locale(): string {
  return LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'en-US'
}

/**
 * The form of a counted word for `n`. Forms are `one|other` in English and
 * `one|few|many` in Ukrainian and Russian; a key without `|` is used as is.
 */
export function plural(t: T, key: string, n: number): string {
  const forms = t(key).split('|')
  if (forms.length === 1) return forms[0]
  const rule = new Intl.PluralRules(locale()).select(n)
  if (forms.length === 2) return rule === 'one' ? forms[0] : forms[1]
  if (rule === 'one') return forms[0]
  if (rule === 'few') return forms[1]
  if (rule === 'many') return forms[2]
  // Fractions; never a count of orders, but never a crash either.
  return forms[1]
}
