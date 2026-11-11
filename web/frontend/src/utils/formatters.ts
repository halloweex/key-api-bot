/**
 * Locale-aware formatters for numbers, currency, dates, and text.
 */
import i18n, { LANGUAGE_LOCALES, type SupportedLanguage } from '../lib/i18n'

function getLocale(): string {
  return LANGUAGE_LOCALES[i18n.language as SupportedLanguage] || 'en-US'
}

/**
 * Format number as Ukrainian Hryvnia currency
 */
export function formatCurrency(value: number): string {
  const formatted = new Intl.NumberFormat(getLocale(), {
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value)
  return `\u20B4${formatted}`
}

/**
 * Hryvnia in as few characters as stay readable, for a tile too narrow to
 * hold the full figure (which it keeps in its tooltip): from a million up
 * `₴68,1 млн` (`₴68.1M` in English), and with no tenth once the figure has
 * three digits before it — `₴109 млн`, not `₴108,5 млн`, which is a
 * character more than a tile beside the sidebar holds and a precision nobody
 * reads at that size; below a million the full figure, which is already the
 * shorter of the two.
 */
export function formatCurrencyShort(value: number): string {
  const size = Math.abs(value)
  if (size < 1_000_000) return formatCurrency(value)
  // The figure before the unit: 108.5 for 108 542 127, 1.2 for 1.2 billion.
  const unit = 10 ** (Math.floor(Math.log10(size) / 3) * 3)
  const compact = new Intl.NumberFormat(getLocale(), {
    notation: 'compact',
    maximumFractionDigits: size / unit >= 100 ? 0 : 1,
  }).format(value)
  return `₴${compact}`
}

/**
 * A report's dates as two numeric days, `01.04 – 10.04.2026`, the start
 * carrying its own year when it differs from the end's
 * (`02.12.2023 – 05.11.2026`: without it the whole history read as a span
 * inside 2026). A full calendar month is its name, `КВІТЕНЬ 2026`.
 */
export function formatReportRange(startDate: string, endDate: string): string {
  const locale = getLocale()
  const start = new Date(startDate + 'T00:00:00')
  const end = new Date(endDate + 'T00:00:00')

  const sameYear = start.getFullYear() === end.getFullYear()
  const sameMonth = sameYear && start.getMonth() === end.getMonth()
  const lastDayOfMonth = new Date(end.getFullYear(), end.getMonth() + 1, 0).getDate()
  if (sameMonth && start.getDate() === 1 && end.getDate() === lastDayOfMonth) {
    return new Intl.DateTimeFormat(locale, { month: 'long', year: 'numeric' }).format(start).toUpperCase()
  }

  const day: Intl.DateTimeFormatOptions = { day: '2-digit', month: '2-digit' }
  const fmtStart = new Intl.DateTimeFormat(locale, sameYear ? day : { ...day, year: 'numeric' }).format(start)
  const fmtEnd = new Intl.DateTimeFormat(locale, { ...day, year: 'numeric' }).format(end)
  return `${fmtStart} – ${fmtEnd}`
}

/**
 * Format number with thousands separator
 */
export function formatNumber(value: number): string {
  return new Intl.NumberFormat(getLocale()).format(value)
}

/**
 * Format percentage
 */
export function formatPercent(value: number, decimals = 1): string {
  return `${value.toFixed(decimals)}%`
}

/**
 * Format date string to localized format
 */
export function formatDate(dateStr: string): string {
  const date = new Date(dateStr)
  return new Intl.DateTimeFormat(getLocale(), {
    day: 'numeric',
    month: 'short',
  }).format(date)
}

/**
 * Format date for API (YYYY-MM-DD)
 */
export function formatDateForApi(date: Date): string {
  return date.toISOString().split('T')[0]
}

/**
 * Truncate text with ellipsis
 */
export function truncate(text: string, maxLength: number): string {
  if (text.length <= maxLength) return text
  return text.slice(0, maxLength - 3) + '...'
}
