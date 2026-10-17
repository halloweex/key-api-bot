// Every export button goes through here, because the dashboard is opened as a
// Telegram Mini App and Telegram's WebView breaks both ways a page normally
// hands over a file:
//
//  * a `blob:` link with `download` — the click does nothing at all;
//  * `window.open(url)` — Telegram passes the URL to the external browser,
//    which has no session and gets a 401 instead of the CSV.
//
// So inside Telegram the page fetches the file itself (same origin, the
// session cookie rides along) and the bot sends it to the user's own chat
// (`POST /api/me/send-file`). In an ordinary browser nothing changes.

import { api } from '../api/client'

/** True inside a Telegram client. Desktop and mobile inject
 * `TelegramWebviewProxy`; Telegram Web hosts the page in an iframe. */
export function insideTelegram(): boolean {
  if (typeof window === 'undefined') return false
  if ('TelegramWebviewProxy' in window) return true
  try {
    return window.self !== window.top
  } catch {
    return true // a cross-origin parent: framed, so Telegram Web
  }
}

export type Delivery = 'downloaded' | 'sent'

function blobDownload(text: string, filename: string) {
  // UTF-8 BOM for Excel Cyrillic compatibility
  const blob = new Blob(['﻿' + text], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  // In the document and revoked later: Firefox ignores a detached anchor, and
  // Safari can lose a URL revoked in the same tick as the click.
  document.body.appendChild(a)
  a.click()
  a.remove()
  setTimeout(() => URL.revokeObjectURL(url), 10_000)
}

/** A CSV the page built itself. */
export async function saveCsv(text: string, filename: string): Promise<Delivery> {
  if (!insideTelegram()) {
    blobDownload(text, filename)
    return 'downloaded'
  }
  await api.sendFileToMyChat(filename, text)
  return 'sent'
}

function filenameFrom(response: Response, fallback: string): string {
  const header = response.headers.get('Content-Disposition') ?? ''
  const match = /filename\*?=(?:UTF-8'')?"?([^";]+)"?/i.exec(header)
  const name = match ? decodeURIComponent(match[1]).trim() : ''
  return /^[A-Za-z0-9._-]+\.csv$/.test(name) ? name : fallback
}

/** A CSV the server renders at `url`. */
export async function exportCsvFromUrl(url: string, fallbackName: string): Promise<Delivery> {
  if (!insideTelegram()) {
    window.open(url, '_blank')
    return 'downloaded'
  }
  const response = await fetch(url, { credentials: 'same-origin' })
  if (!response.ok) {
    throw new Error(`Export failed (HTTP ${response.status})`)
  }
  const text = await response.text()
  await api.sendFileToMyChat(filenameFrom(response, fallbackName), text)
  return 'sent'
}
