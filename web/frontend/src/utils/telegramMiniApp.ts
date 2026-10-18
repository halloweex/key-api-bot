/**
 * The dashboard inside Telegram's Mini App, and the two native buttons there.
 *
 * The owner builds campaigns from a phone, often inside the Telegram Mini App.
 * There the bottom of the screen is Telegram's own: a MainButton under the
 * page and a BackButton in the header, which a person reaches for before a
 * button drawn on the page. The wizard drives them — «Далее · N получателей»
 * as the MainButton, the BackButton closing a sheet and then stepping back —
 * and outside Telegram nothing here does anything at all.
 *
 * The page does not load Telegram's SDK in a browser. Inside Telegram it is
 * fetched once, from the origin the CSP already allows for the login page
 * (`script-src https://telegram.org`), and only drives the buttons when the
 * SDK says it is in a real client (`platform` known) of a version that has
 * both of them (6.1).
 */

export interface TelegramMainButton {
  setParams(params: { text?: string; is_active?: boolean; is_visible?: boolean }): unknown
  onClick(callback: () => void): unknown
  offClick(callback: () => void): unknown
  hide(): unknown
}

export interface TelegramBackButton {
  onClick(callback: () => void): unknown
  offClick(callback: () => void): unknown
  show(): unknown
  hide(): unknown
}

export interface TelegramWebApp {
  platform: string
  isVersionAtLeast(version: string): boolean
  MainButton: TelegramMainButton
  BackButton: TelegramBackButton
}

declare global {
  interface Window {
    Telegram?: { WebApp?: TelegramWebApp }
  }
}

const SDK = 'https://telegram.org/js/telegram-web-app.js'
const SDK_TIMEOUT_MS = 5000

/**
 * True inside a Telegram client. Desktop and mobile inject
 * `TelegramWebviewProxy`; Telegram Web hosts the page in an iframe.
 *
 * The file exports' test (`utils/deliverFile.ts`, PR #289) with one narrowing:
 * a frame counts only when its parent is another origin. A same-origin parent
 * is Storybook or a preview, never Telegram, and loading Telegram's script
 * into it would be a request to telegram.org for nothing. (Production cannot
 * be framed by anyone else at all — `frame-ancestors 'self'` — so the frame
 * branch is for completeness; the proxy is what a phone and a desktop have.)
 * When both are on main, one of the two should import the other.
 */
export function insideTelegram(): boolean {
  if (typeof window === 'undefined') return false
  if ('TelegramWebviewProxy' in window) return true
  if (window.self === window.top) return false
  try {
    // Readable only from the same origin.
    return !window.top?.location.href
  } catch {
    return true // a cross-origin parent: framed, so Telegram Web
  }
}

let loading: Promise<TelegramWebApp | null> | null = null

/** The SDK's WebApp, loaded once; null outside Telegram or if it cannot load. */
export function loadTelegramWebApp(): Promise<TelegramWebApp | null> {
  if (!insideTelegram()) return Promise.resolve(null)
  if (window.Telegram?.WebApp) return Promise.resolve(window.Telegram.WebApp)
  if (loading) return loading
  loading = new Promise((resolve) => {
    const script = document.createElement('script')
    const timer = setTimeout(() => resolve(null), SDK_TIMEOUT_MS)
    script.src = SDK
    script.async = true
    script.onload = () => { clearTimeout(timer); resolve(window.Telegram?.WebApp ?? null) }
    script.onerror = () => { clearTimeout(timer); resolve(null) }
    document.head.appendChild(script)
  })
  return loading
}

/** The WebApp when its two buttons can be driven, else null. */
export function usableWebApp(webApp: TelegramWebApp | null | undefined): TelegramWebApp | null {
  if (!webApp) return null
  // The SDK defines itself anywhere it is loaded; "unknown" is a page that is
  // framed but not by Telegram (Storybook, a preview).
  if (!webApp.platform || webApp.platform === 'unknown') return null
  try {
    return webApp.isVersionAtLeast('6.1') ? webApp : null
  } catch {
    return null
  }
}

/** Only for tests: forget the loaded SDK. */
export function resetTelegramWebAppForTests(): void {
  loading = null
}
