import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import {
  insideTelegram, loadTelegramWebApp, resetTelegramWebAppForTests, usableWebApp,
  type TelegramWebApp,
} from '../telegramMiniApp'

/**
 * The one answer to "is this page inside Telegram" — the wizard's native
 * buttons and every file export ask it. A wrong yes in Storybook would post a
 * CSV to a chat and fetch Telegram's script for nothing; a wrong no in the
 * app would leave an export that saves nothing.
 */

const SDK = 'https://telegram.org/js/telegram-web-app.js'
const topDescriptor = Object.getOwnPropertyDescriptor(window, 'top')

/** The page in a frame: `href` readable for a same-origin parent, a throw for
 * any other origin, as a browser does it. */
function framedBy(origin: 'same' | 'cross') {
  const parent = origin === 'same'
    ? { location: { href: 'http://localhost:6006/iframe.html' } }
    : { get location(): Location { throw new DOMException('Blocked a frame', 'SecurityError') } }
  Object.defineProperty(window, 'top', { configurable: true, get: () => parent })
}

function sdkScripts() {
  return document.head.querySelectorAll(`script[src="${SDK}"]`)
}

beforeEach(() => {
  resetTelegramWebAppForTests()
})

afterEach(() => {
  if (topDescriptor) Object.defineProperty(window, 'top', topDescriptor)
  delete (window as { TelegramWebviewProxy?: object }).TelegramWebviewProxy
  delete (window as { Telegram?: object }).Telegram
  sdkScripts().forEach((s) => s.remove())
})

describe('insideTelegram', () => {
  it('is false for a page of its own in a browser', () => {
    expect(insideTelegram()).toBe(false)
  })

  it('is true where a Telegram client injected its proxy', () => {
    ;(window as unknown as { TelegramWebviewProxy: object }).TelegramWebviewProxy = {}
    expect(insideTelegram()).toBe(true)
  })

  it('is true in a frame of another origin, as Telegram Web hosts it', () => {
    framedBy('cross')
    expect(insideTelegram()).toBe(true)
  })

  it('is false in a frame of its own origin: Storybook, a preview', () => {
    framedBy('same')
    expect(insideTelegram()).toBe(false)
  })
})

describe('loadTelegramWebApp', () => {
  it('asks telegram.org for nothing outside Telegram', async () => {
    expect(await loadTelegramWebApp()).toBeNull()
    framedBy('same')
    expect(await loadTelegramWebApp()).toBeNull()
    expect(sdkScripts()).toHaveLength(0)
  })

  it('uses a WebApp already on the page without loading another', async () => {
    const webApp = { platform: 'ios' } as TelegramWebApp
    ;(window as unknown as { TelegramWebviewProxy: object }).TelegramWebviewProxy = {}
    ;(window as unknown as { Telegram: object }).Telegram = { WebApp: webApp }
    expect(await loadTelegramWebApp()).toBe(webApp)
    expect(sdkScripts()).toHaveLength(0)
  })

  it('loads the script once however many ask', async () => {
    ;(window as unknown as { TelegramWebviewProxy: object }).TelegramWebviewProxy = {}
    const first = loadTelegramWebApp()
    const second = loadTelegramWebApp()
    expect(sdkScripts()).toHaveLength(1)

    const webApp = { platform: 'android' } as TelegramWebApp
    ;(window as unknown as { Telegram: object }).Telegram = { WebApp: webApp }
    sdkScripts()[0].dispatchEvent(new Event('load'))
    expect(await first).toBe(webApp)
    expect(await second).toBe(webApp)
  })

  it('answers null when the script cannot load', async () => {
    ;(window as unknown as { TelegramWebviewProxy: object }).TelegramWebviewProxy = {}
    const loading = loadTelegramWebApp()
    sdkScripts()[0].dispatchEvent(new Event('error'))
    expect(await loading).toBeNull()
  })
})

describe('usableWebApp', () => {
  const app = (platform: string, version: boolean | 'throws') => ({
    platform,
    isVersionAtLeast: () => {
      if (version === 'throws') throw new Error('old SDK')
      return version
    },
  }) as unknown as TelegramWebApp

  it('takes a real client of a version with both buttons', () => {
    const webApp = app('ios', true)
    expect(usableWebApp(webApp)).toBe(webApp)
  })

  it('refuses a page the SDK cannot place, an old client, or no SDK', () => {
    expect(usableWebApp(app('unknown', true))).toBeNull()
    expect(usableWebApp(app('', true))).toBeNull()
    expect(usableWebApp(app('ios', false))).toBeNull()
    expect(usableWebApp(app('ios', 'throws'))).toBeNull()
    expect(usableWebApp(null)).toBeNull()
  })
})
