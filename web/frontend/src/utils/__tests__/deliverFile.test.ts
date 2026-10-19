import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { exportCsvFromUrl, saveCsv } from '../deliverFile'

/**
 * Every export button ends here. In a browser it downloads exactly as before;
 * inside Telegram, whose WebView saves nothing, the page fetches the file and
 * the bot sends it to the requester's own chat. Which of the two is decided by
 * `telegramMiniApp.insideTelegram`, the test the wizard's buttons use as well.
 */

const sent: Array<[string, string]> = []

vi.mock('../../api/client', () => ({
  api: {
    sendFileToMyChat: vi.fn(async (filename: string, content: string) => {
      sent.push([filename, content])
      return { sent: true }
    }),
  },
}))

const topDescriptor = Object.getOwnPropertyDescriptor(window, 'top')

function inTelegram() {
  ;(window as unknown as { TelegramWebviewProxy: object }).TelegramWebviewProxy = {}
}

function inStorybook() {
  const parent = { location: { href: 'http://localhost:6006/iframe.html' } }
  Object.defineProperty(window, 'top', { configurable: true, get: () => parent })
}

let createObjectURL: ReturnType<typeof vi.fn>
let open: ReturnType<typeof vi.spyOn>

beforeEach(() => {
  sent.length = 0
  createObjectURL = vi.fn(() => 'blob:x')
  Object.assign(URL, { createObjectURL, revokeObjectURL: vi.fn() })
  open = vi.spyOn(window, 'open').mockImplementation(() => null)
  vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => {})
})

afterEach(() => {
  if (topDescriptor) Object.defineProperty(window, 'top', topDescriptor)
  delete (window as { TelegramWebviewProxy?: object }).TelegramWebviewProxy
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

describe('saveCsv', () => {
  it('downloads in a browser, as it always did', async () => {
    expect(await saveCsv('a,b', 'sku.csv')).toBe('downloaded')
    expect(createObjectURL).toHaveBeenCalledTimes(1)
    expect(sent).toEqual([])
  })

  it('has the bot send it inside Telegram, and builds no blob link', async () => {
    inTelegram()
    expect(await saveCsv('a,b', 'sku.csv')).toBe('sent')
    expect(sent).toEqual([['sku.csv', 'a,b']])
    expect(createObjectURL).not.toHaveBeenCalled()
  })

  it('downloads in Storybook: a same-origin frame is not Telegram', async () => {
    inStorybook()
    expect(await saveCsv('a,b', 'sku.csv')).toBe('downloaded')
    expect(sent).toEqual([])
  })
})

describe('exportCsvFromUrl', () => {
  const URL_ = '/api/reports/export/csv?period=week'

  it('opens the link in a browser, as it always did', async () => {
    const fetchSpy = vi.fn()
    vi.stubGlobal('fetch', fetchSpy)
    expect(await exportCsvFromUrl(URL_, 'report.csv')).toBe('downloaded')
    expect(open).toHaveBeenCalledWith(URL_, '_blank')
    expect(fetchSpy).not.toHaveBeenCalled()
  })

  it('fetches with the session inside Telegram and sends what it got', async () => {
    inTelegram()
    const fetchSpy = vi.fn(async () => new Response('x,y', {
      headers: { 'Content-Disposition': 'attachment; filename="report-summary-2026-10-19.csv"' },
    }))
    vi.stubGlobal('fetch', fetchSpy)
    expect(await exportCsvFromUrl(URL_, 'report.csv')).toBe('sent')
    expect(fetchSpy).toHaveBeenCalledWith(URL_, { credentials: 'same-origin' })
    expect(sent).toEqual([['report-summary-2026-10-19.csv', 'x,y']])
    expect(open).not.toHaveBeenCalled()
  })

  it('keeps the fallback name when the server offers no plain CSV name', async () => {
    inTelegram()
    vi.stubGlobal('fetch', vi.fn(async () => new Response('x', {
      headers: { 'Content-Disposition': 'attachment; filename="../evil.sh"' },
    })))
    await exportCsvFromUrl(URL_, 'report.csv')
    expect(sent).toEqual([['report.csv', 'x']])
  })

  it('sends nothing when the server refused the export', async () => {
    inTelegram()
    vi.stubGlobal('fetch', vi.fn(async () => new Response('denied', { status: 403 })))
    await expect(exportCsvFromUrl(URL_, 'report.csv')).rejects.toThrow(/HTTP 403/)
    expect(sent).toEqual([])
  })

  it('opens the link in Storybook: a same-origin frame is not Telegram', async () => {
    inStorybook()
    expect(await exportCsvFromUrl(URL_, 'report.csv')).toBe('downloaded')
    expect(open).toHaveBeenCalledWith(URL_, '_blank')
  })
})
