/**
 * Telegram's MainButton and BackButton, driven by a page that is open now.
 *
 * Each hook mirrors what the page already shows in one of its own buttons —
 * the same label, the same enabled state, the same handler — and hides the
 * native one again when the page stops asking for it or unmounts, so a
 * button never outlives the screen it belonged to. Outside the Mini App
 * (`usableWebApp` null) both do nothing.
 */
import { useEffect, useRef, useState } from 'react'
import {
  loadTelegramWebApp, usableWebApp, type TelegramWebApp,
} from '../utils/telegramMiniApp'

/** The Mini App's WebApp once it is known to drive buttons, else null. */
export function useTelegramWebApp(): TelegramWebApp | null {
  const [webApp, setWebApp] = useState<TelegramWebApp | null>(
    () => usableWebApp(typeof window === 'undefined' ? null : window.Telegram?.WebApp),
  )
  useEffect(() => {
    if (webApp) return
    let live = true
    void loadTelegramWebApp().then((loaded) => {
      if (live) setWebApp(usableWebApp(loaded))
    })
    return () => { live = false }
  }, [webApp])
  return webApp
}

/** Telegram's MainButton as the page's primary action, while `visible`. */
export function useTelegramMainButton(
  webApp: TelegramWebApp | null,
  { visible, text, enabled, onClick }: {
    visible: boolean; text: string; enabled: boolean; onClick: () => void
  },
): void {
  const handler = useRef(onClick)
  useEffect(() => { handler.current = onClick }, [onClick])

  // One listener for the button's life; it calls whatever the page's action
  // is now, and does nothing while the action is disabled — the native button
  // can be pressed in the instant before it greys out.
  const active = useRef(enabled && visible)
  useEffect(() => { active.current = enabled && visible }, [enabled, visible])
  useEffect(() => {
    if (!webApp) return
    const press = () => { if (active.current) handler.current() }
    webApp.MainButton.onClick(press)
    return () => {
      webApp.MainButton.offClick(press)
      webApp.MainButton.hide()
    }
  }, [webApp])

  useEffect(() => {
    if (!webApp) return
    if (!visible) {
      webApp.MainButton.hide()
      return
    }
    webApp.MainButton.setParams({ text, is_active: enabled, is_visible: true })
  }, [webApp, visible, text, enabled])
}

/** Telegram's BackButton, while `visible`, calling `onClick`. */
export function useTelegramBackButton(
  webApp: TelegramWebApp | null,
  { visible, onClick }: { visible: boolean; onClick: () => void },
): void {
  const handler = useRef(onClick)
  useEffect(() => { handler.current = onClick }, [onClick])
  useEffect(() => {
    if (!webApp) return
    const press = () => handler.current()
    webApp.BackButton.onClick(press)
    return () => {
      webApp.BackButton.offClick(press)
      webApp.BackButton.hide()
    }
  }, [webApp])
  useEffect(() => {
    if (!webApp) return
    if (visible) webApp.BackButton.show()
    else webApp.BackButton.hide()
  }, [webApp, visible])
}
